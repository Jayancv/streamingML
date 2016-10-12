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


    private int maxInstance = 1000000;
    private int batchSize = 500;
    private int numClasses = 2;
    private int paramCount = 9;
    private int numNominals = 0;
    private String nominalAttVals = "";
    private int paramPosition = 0;
    private int parallelism=0;
    private int numModelsBagging=0;
    private StreamingClassification streamingClassification = null;

    List<String> classes = new ArrayList<String>();           //values of class attribute
    ArrayList<ArrayList<String>> nominals = new ArrayList<ArrayList<String>>();     //values of other nominal attributes


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
                numClasses = ((Integer) attributeExpressionExecutors[2].execute(null));
                paramCount = ((Integer) attributeExpressionExecutors[3].execute(null));
                numNominals = ((Integer) attributeExpressionExecutors[4].execute(null));
                nominalAttVals = ((String) attributeExpressionExecutors[5].execute(null));
                parallelism = ((Integer) attributeExpressionExecutors[6].execute(null));
                numModelsBagging = ((Integer) attributeExpressionExecutors[7].execute(null));


            } catch (ClassCastException c) {
                throw new ExecutionPlanCreationException("should be of type int");
            }
        }
        System.out.println("StreamingClassification  Parameters: " + " Maximum instances = " + maxInstance + ", Batch size =  " + batchSize +" , Number of classes = "+numClasses+ "\n");
        streamingClassification = new StreamingClassification(maxInstance, batchSize, numClasses, paramCount, numNominals, nominalAttVals,parallelism,numModelsBagging);
        try {
            Thread.sleep(1000);
        } catch (Exception e) {

        }
        new Thread(streamingClassification).start();

        // Add attributes
        String betaVal;
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(8);
        attributes.add(new Attribute("numInstance", Attribute.Type.INT));
        attributes.add(new Attribute("correctness", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("kappa", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("tempKappa", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("support", Attribute.Type.STRING));
        attributes.add(new Attribute("precision", Attribute.Type.STRING));
        attributes.add(new Attribute("recall", Attribute.Type.STRING));
        attributes.add(new Attribute("f1score", Attribute.Type.STRING));


        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {


        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();

                Object[] inputData = new Object[attributeExpressionLength - paramPosition];
                Double[] eventData = new Double[attributeExpressionLength - paramPosition];
                double[] cepEvent = new double[attributeExpressionLength - paramPosition];
                Object evt;
                Double value;

//                for (int i = paramPosition; i < attributeExpressionLength; i++) {
//                    inputData[i - paramPosition] = attributeExpressionExecutors[i].execute(complexEvent);
//                    value = eventData[i - paramPosition] = (Double) attributeExpressionExecutors[i].execute(complexEvent);
//                    cepEvent[i - paramPosition] = (double) value;
//                }


                String classValue = (String) attributeExpressionExecutors[attributeExpressionLength - 1].execute(complexEvent).toString();
                if (classes.contains(classValue)) {
                    cepEvent[paramCount - 1] = classes.indexOf(classValue);
                } else {
                    classes.add(classValue);
                    cepEvent[paramCount - 1] = classes.indexOf(classValue);
                }
                int j = 0;

                for (int i = 0; i < paramCount - 1; i++) {
                    evt =attributeExpressionExecutors[i+paramPosition].execute(complexEvent);
                    inputData[i]=evt;
                    if (i < paramCount - 1 - numNominals) {
                        value=eventData[i]=(Double)evt;
                        cepEvent[i] = value;
                    } else {
                        String v = (String) evt;
                        try {
                            if (!nominals.get(j).contains(evt)) {
                                nominals.get(j).add(v);
                            }
                        } catch (IndexOutOfBoundsException e) {
                            nominals.add(new ArrayList<String>());
                            nominals.get(j).add(v);
                        }
                        cepEvent[i] = (nominals.get(j).indexOf(v));
                        j++;
                    }
                }
                Object[] outputData = null;

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
