package org.wso2.carbon.ml.siddhi.extension.streamingml;

import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassification;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression.StreamingRegression;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wso2123 on 10/12/16.
 */
public class StreamingRegressionWithSamoaStreamProcessor extends StreamProcessor implements SchedulingProcessor {


    private int maxInstance = 1000000;
    private int batchSize = 500;
    private int paramCount = 9;
    private int paramPosition = 0;
    private int parallelism = 0;
    private int numModelsBagging = 0;
    private StreamingRegression streamingRegression = null;

    private ExpressionExecutor timestampExecutor;
    private long lastScheduledTimestamp = -1;
    private long TIMER_DURATION = 100;
    private Scheduler scheduler;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        paramCount = attributeExpressionLength;
        int PARAM_WIDTH = 5;

        // Capture constant inputs
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {

            paramCount = paramCount - PARAM_WIDTH;
            paramPosition = PARAM_WIDTH;
            try {
                maxInstance = ((Integer) attributeExpressionExecutors[0].execute(null));
                batchSize = ((Integer) attributeExpressionExecutors[1].execute(null));
                paramCount = ((Integer) attributeExpressionExecutors[2].execute(null));
                parallelism = ((Integer) attributeExpressionExecutors[3].execute(null));
                numModelsBagging = ((Integer) attributeExpressionExecutors[4].execute(null));
            } catch (ClassCastException c) {
                throw new ExecutionPlanCreationException("should be of type int");
            }
        }
        lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime();

        System.out.println("StreamingRegression  Parameters: " + " Maximum instances = " + maxInstance + ", Batch size =  " + batchSize + "\n");
        streamingRegression = new StreamingRegression(maxInstance, batchSize, paramCount, parallelism, numModelsBagging);
        try {
            Thread.sleep(1000);
        } catch (Exception e) {

        }
        new Thread(streamingRegression).start();

        // Add attributes
        String betaVal;
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(paramCount);
        for (int i = 0; i < paramCount - 1; i++) {
            attributes.add(new Attribute("att_" + i, Attribute.Type.DOUBLE));
        }
        attributes.add(new Attribute("prediction", Attribute.Type.DOUBLE));
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                if (complexEvent.getType() != ComplexEvent.Type.TIMER) {
                    double[] cepEvent = new double[attributeExpressionLength - paramPosition];
                    Object evt;
                    Double value;

                    Object classVal = attributeExpressionExecutors[attributeExpressionLength - 1].execute(complexEvent);
//                    if (classVal.toString().equals("-0.0")) {
//                        //System.out.println("got -0");
//                    } else {
                    double classValue = (double) classVal;
                    if (classValue == -1.0) {           //this data points for prediction
                        cepEvent[paramCount - 1] = -1;
                    } else {
                        cepEvent[paramCount - 1] = classValue;
                    }
//                    }
                    int j = 0;

                    for (int i = 0; i < paramCount; i++) {
                        evt = attributeExpressionExecutors[i + paramPosition].execute(complexEvent);
                        cepEvent[i] = (double) evt;
                    }

                    Object[] outputData = null;
                    outputData = streamingRegression.regress(cepEvent);

                    if (outputData == null) {
                        streamEventChunk.remove();
                    } else {
                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    }

                } else if (complexEvent.getType() == ComplexEvent.Type.TIMER) {
                    //System.out.println("Time");
                    lastScheduledTimestamp = lastScheduledTimestamp + TIMER_DURATION;
                    scheduler.notifyAt(lastScheduledTimestamp);

                    Object[] outputData = null;
                    outputData = streamingRegression.regress();

                    if (outputData == null) {
                        streamEventChunk.remove();
                    } else {
                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    }
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }


    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {

    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        if (lastScheduledTimestamp > 0) {
            lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime() + TIMER_DURATION;
            scheduler.notifyAt(lastScheduledTimestamp);
        }

    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }
}
