/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.ml.siddhi.extension.streamingml;

import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification.StreamingClassification;
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
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.List;

public class StreamingClassificationWithSamoaStreamProcessor extends StreamProcessor implements SchedulingProcessor {

    private int maxInstance = 1000000;
    private int batchSize = 500;
    private int numClasses = 2;
    private int paramCount = 9;
    private int numNominals = 0;
    private String nominalAttVals = "";
    private int paramPosition = 0;
    private int parallelism = 0;
    private int numModelsBagging = 0;
    private StreamingClassification streamingClassification = null;
    private long lastScheduledTimestamp = -1;
    private long TIMER_DURATION = 100;
    private Scheduler scheduler;

    List<String> classes = new ArrayList<String>();           //values of class attribute
    ArrayList<ArrayList<String>> nominals = new ArrayList<ArrayList<String>>();     //values of other nominal attributes

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors
            , ExecutionPlanContext executionPlanContext) {
        paramCount = attributeExpressionLength;
        int PARAM_WIDTH = 8;

        if (attributeExpressionExecutors.length > PARAM_WIDTH +1) {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                maxInstance = ((Integer) attributeExpressionExecutors[0].execute(null));
                if(maxInstance==-1){
                    maxInstance=Integer.MAX_VALUE;
                }
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the first argument, " +
                        "required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[0].getReturnType().toString());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                batchSize = ((Integer) attributeExpressionExecutors[1].execute(null));
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the second argument, " +
                        "required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[1].getReturnType().toString());
            }

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                numClasses = ((Integer) attributeExpressionExecutors[2].execute(null));
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the third argument, " +
                        "required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[2].getReturnType().toString());
            }

            if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT) {
                paramCount = ((Integer) attributeExpressionExecutors[3].execute(null));
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the fourth argument, " +
                        "required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[3].getReturnType().toString());
            }

            if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.INT) {
                numNominals = ((Integer) attributeExpressionExecutors[4].execute(null));
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the fifth argument, " +
                        "required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[4].getReturnType().toString());
            }

            if (attributeExpressionExecutors[5].getReturnType() == Attribute.Type.STRING) {
                nominalAttVals = ((String) attributeExpressionExecutors[5].execute(null));
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the sixth argument, " +
                        "required " + Attribute.Type.STRING + " but found " +
                        attributeExpressionExecutors[5].getReturnType().toString());
            }

            if (attributeExpressionExecutors[6].getReturnType() == Attribute.Type.INT) {
                parallelism = ((Integer) attributeExpressionExecutors[6].execute(null));
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the seventh argument, " +
                        "required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[6].getReturnType().toString());
            }

            if (attributeExpressionExecutors[7].getReturnType() == Attribute.Type.INT) {
                numModelsBagging = ((Integer) attributeExpressionExecutors[7].execute(null));
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the eighth argument, " +
                        "required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[7].getReturnType().toString());
            }
            String[] temp = nominalAttVals.split(",");
            if (temp.length != numNominals && numNominals != 0) {
                throw new ExecutionPlanValidationException("Number of nominal attributes and entered nominal attributes" +
                        " are different. required " + numNominals + " but found " + temp.length);
            }

            if (attributeExpressionExecutors.length != (PARAM_WIDTH + paramCount)) {
                throw new ExecutionPlanValidationException("Number of attribute and entered(Input stream) attributes" +
                        " are different. required " + paramCount + " but found " + (attributeExpressionExecutors.length
                        - PARAM_WIDTH));
            }

        } else {
            throw new ExecutionPlanValidationException("Invalid no of arguments, required more than "+(PARAM_WIDTH +2)+
                    "(Attribute count should be equal or grater than 2), but found " +
                    attributeExpressionExecutors.length + " : #streamingml:streamingClassificationSamoa(numMaxEvents," +
                    " displayIntervale, numClasses, parCount , numNominals, valuesOfNominals, parallelism, " +
                    "ensembleSize, attribute_set)");
        }

        paramPosition = PARAM_WIDTH;
        lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime();
        streamingClassification = new StreamingClassification(maxInstance, batchSize, numClasses, paramCount,
                numNominals, nominalAttVals, parallelism, numModelsBagging);

        new Thread(streamingClassification).start();

        ArrayList<Attribute> attributes = new ArrayList<Attribute>(paramCount);
        for (int i = 0; i < paramCount - 1; i++) {
            if (i < paramCount - numNominals - 1)
                attributes.add(new Attribute("att_" + i, Attribute.Type.DOUBLE));
            else
                attributes.add(new Attribute("att_" + i, Attribute.Type.STRING));
        }
        attributes.add(new Attribute("prediction", Attribute.Type.STRING));
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor
            nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                if (complexEvent.getType() != ComplexEvent.Type.TIMER) {
                    double[] cepEvent = new double[attributeExpressionLength - paramPosition];
                    Object evt;
                    Double value;

                    // Set cep event values
                    String classValue = (String) attributeExpressionExecutors[attributeExpressionLength - 1].
                            execute(complexEvent).toString();
                    //Set class value
                    if (classValue.equals("?")) {           //these data points for prediction
                        cepEvent[paramCount - 1] = -1;
                    } else { // These data points have class values, therefore these data use to train and test the model
                        if (classes.contains(classValue)) {
                            cepEvent[paramCount - 1] = classes.indexOf(classValue);
                        } else {
                            if (classes.size() < numClasses) {
                                classes.add(classValue);
                                cepEvent[paramCount - 1] = classes.indexOf(classValue);
                            }
                        }
                    }
                    int j = 0;

                    // Set other attributes
                    for (int i = 0; i < paramCount - 1; i++) {
                        evt = attributeExpressionExecutors[i + paramPosition].execute(complexEvent);

                        if (i < paramCount - 1 - numNominals) {                       // set Numerical attributes
                            value = (Double) evt;
                            cepEvent[i] = value;
                        } else {                                                      // Set nominal attributes
                            String v = (String) evt;
                            if (nominals.size() < j) {
                                if (!nominals.get(j).contains(evt)) {
                                    nominals.get(j).add(v);
                                }
                            } else {
                                nominals.add(new ArrayList<String>());
                                nominals.get(j).add(v);
                            }
                            cepEvent[i] = (nominals.get(j).indexOf(v));
                            j++;
                        }
                    }
                    Object[] outputData = null;
                    outputData = streamingClassification.classify(cepEvent);          // get Output

                    if (outputData == null) {
                        streamEventChunk.remove();
                    } else {                          // If output have values, then add thoses values to output stream
                        int index_predic = (int) outputData[outputData.length - 1];
                        outputData[outputData.length - 1] = classes.get(index_predic);
                        if (numNominals != 0) {
                            for (int k = paramCount - numNominals - 1; k < paramCount - 1; k++) {
                                int nominal_index = (int) outputData[k];
                                outputData[k] = nominals.get(k - paramCount - numNominals - 1).get(nominal_index);
                            }
                        }
                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    }

                } else if (complexEvent.getType() == ComplexEvent.Type.TIMER) { // Timer events for poll output events from prediction queue
                    lastScheduledTimestamp = lastScheduledTimestamp + TIMER_DURATION;
                    scheduler.notifyAt(lastScheduledTimestamp);
                    Object[] outputData = null;
                    outputData = streamingClassification.getClassify();

                    if (outputData == null) {
                        streamEventChunk.remove();
                    } else {
                        int index_predic = (int) outputData[outputData.length - 1];
                        outputData[outputData.length - 1] = classes.get(index_predic);
                        if (numNominals != 0) {
                            for (int k = paramCount - numNominals - 1; k < paramCount - 1; k++) {
                                int nominal_index = (int) outputData[k];
                                outputData[k] = nominals.get(k - paramCount - numNominals - 1).get(nominal_index);
                            }
                        }
                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    }

                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }


    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {
        //Do nothing
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
        return this.scheduler;
    }
}
