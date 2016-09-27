package org.wso2.carbon.ml.siddhi.extension.streamingml;

import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassification;
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
 * Created by wso2123 on 8/29/16.
 */
public class StreamingClassificationWithSamoaStreamProcessor extends StreamProcessor {


    private int maxInstance=1000000;
    private int numClasses=2;
    private int paramCount = 9;
    private int batchSize = 500;
    private int paramPosition = 0;
    private StreamingClassification streamingClassification =null;


    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        paramCount = attributeExpressionLength;
        int PARAM_WIDTH=2;

        // Capture constant inputs
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {

            paramCount = paramCount - PARAM_WIDTH;
            paramPosition = PARAM_WIDTH;
            try {
                maxInstance = ((Integer) attributeExpressionExecutors[0].execute(null));
                batchSize = ((Integer) attributeExpressionExecutors[1].execute(null));

            } catch (ClassCastException c) {
                throw new ExecutionPlanCreationException("maximum no of instance, step size should be of type int");
            }
        }
        System.out.println("StreamingClassification  Parameters: "+" "+maxInstance+" "+" "+batchSize+"\n");
        streamingClassification = new StreamingClassification(maxInstance, batchSize,numClasses, paramCount);
        try {
            Thread.sleep(1000);
        }catch(Exception e){

        }
        new Thread(streamingClassification).start();

        // Add attributes for standard error and all beta values
        String betaVal;
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(4);
        attributes.add(new Attribute("numInstance", Attribute.Type.INT));
        attributes.add(new Attribute("correctness", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("kappa", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("tempKappa", Attribute.Type.DOUBLE));


        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();

                Object[] inputData = new Object[attributeExpressionLength - paramPosition];
                Double[] eventData = new Double[attributeExpressionLength - paramPosition];
                double [] cepEvent = new double[attributeExpressionLength - paramPosition];
                Double value;
                for (int i = paramPosition; i < attributeExpressionLength; i++) {
                    inputData[i - paramPosition] = attributeExpressionExecutors[i].execute(complexEvent);
                    value=eventData[i - paramPosition] = (Double) attributeExpressionExecutors[i].execute(complexEvent);
                    cepEvent[i - paramPosition] = (double)value;
                }

               // System.out.println(cepEvent);
                //Object[] outputData = regressionCalculator.calculateLinearRegression(inputData);
                Object[] outputData = null;

                // Object[] outputData= streamingLinearRegression.addToRDD(eventData);
                //Calling the regress function
               // System.out.println("Process");
                outputData = streamingClassification.classify(cepEvent);

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
