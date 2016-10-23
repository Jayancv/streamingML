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
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wso2123 on 10/12/16.
 */
public class StreamingRegressionWithSamoaStreamProcessor extends StreamProcessor {


    private int maxInstance = 1000000;
    private int batchSize = 500;
    private int paramCount = 9;
    private int paramPosition = 0;
    private int parallelism = 0;
    private int numModelsBagging = 0;
    private StreamingRegression streamingRegression = null;

    // List<String> classes = new ArrayList<String>();           //values of class attribute
    // ArrayList<ArrayList<String>> nominals = new ArrayList<ArrayList<String>>();     //values of other nominal attributes


    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        paramCount = attributeExpressionLength;
        int PARAM_WIDTH = 8;

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
        System.out.println("StreamingRegression  Parameters: " + " Maximum instances = " + maxInstance + ", Batch size =  " + batchSize + "\n");
        streamingRegression = new StreamingRegression(maxInstance, batchSize, paramCount, parallelism, numModelsBagging);
        try {
            Thread.sleep(1000);
        } catch (Exception e) {

        }
        new Thread(streamingRegression).start();

        // Add attributes
        String betaVal;
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(5);
        attributes.add(new Attribute("weightObserved", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("squareError", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("averageError", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("squareTargetError", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("averageTargetError", Attribute.Type.DOUBLE));


        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();

             //   Object[] inputData = new Object[attributeExpressionLength - paramPosition];
             //   Double[] eventData = new Double[attributeExpressionLength - paramPosition];
                double[] cepEvent = new double[attributeExpressionLength - paramPosition];
                Object evt;
                Double value;


//                double classValue = (double) attributeExpressionExecutors[attributeExpressionLength - 1].execute(complexEvent);
//                cepEvent[paramCount - 1] = classValue;

                int j = 0;

                for (int i = 0; i < paramCount; i++) {
                    evt = attributeExpressionExecutors[i + paramPosition].execute(complexEvent);
                    //inputData[i] = evt;
                    //value = eventData[i] = (Double) evt;
                    cepEvent[i] = (double) evt;
                }

                Object[] outputData = null;

                outputData = StreamingRegression.regress(cepEvent);

                // Skip processing if user has specified calculation interval
                if (outputData == null) {
                    streamEventChunk.remove();
                } else {
                    complexEventPopulater.populateComplexEvent(complexEvent, outputData);
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
}
