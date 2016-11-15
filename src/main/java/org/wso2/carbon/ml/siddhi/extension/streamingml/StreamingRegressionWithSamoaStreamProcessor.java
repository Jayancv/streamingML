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

import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.regression.StreamingRegression;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
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

public class StreamingRegressionWithSamoaStreamProcessor extends StreamProcessor implements SchedulingProcessor {

    private int maxInstance = 1000000;
    private int batchSize = 500;
    private int paramCount = 9;
    private int paramPosition = 0;
    private int parallelism = 0;
    private StreamingRegression streamingRegression = null;
    private long lastScheduledTimestamp = -1;
    private long TIMER_DURATION = 100;
    private Scheduler scheduler;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                   ExecutionPlanContext executionPlanContext) {
        paramCount = attributeExpressionLength;
        int PARAM_WIDTH = 4;
        if (attributeExpressionExecutors.length > PARAM_WIDTH+1) {
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
                paramCount = ((Integer) attributeExpressionExecutors[2].execute(null));
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the third  argument, " +
                        "required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[2].getReturnType().toString());
            }


            if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT) {
                parallelism = ((Integer) attributeExpressionExecutors[3].execute(null));
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the fourth argument, " +
                        "required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[3].getReturnType().toString());
            }


            if (attributeExpressionExecutors.length != (PARAM_WIDTH + paramCount)) {
                throw new ExecutionPlanValidationException("Number of attribute and entered(Input stream) attributes" +
                        " are different. required " + paramCount + " but found " +
                        (attributeExpressionExecutors.length - PARAM_WIDTH));
            }

        } else {
            throw new ExecutionPlanValidationException("Invalid no of arguments, required more than " + (PARAM_WIDTH+2)+
                    ", but found " + attributeExpressionExecutors.length + "(Attribute count should be equal or grater" +
                    " than 2) : #streamingml:streamingRegressionSamoa(numMaxEvents, displayIntervale,parCount " +
                    ", parallelism, attribute_set)");
        }

        paramPosition = PARAM_WIDTH;
        lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime();
        streamingRegression = new StreamingRegression(maxInstance, batchSize, paramCount, parallelism);
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
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                if (complexEvent.getType() != ComplexEvent.Type.TIMER) {
                    double[] cepEvent = new double[attributeExpressionLength - paramPosition];
                    Object evt;

                    Object classVal = attributeExpressionExecutors[attributeExpressionLength - 1].execute(complexEvent);
                    double classValue = (double) classVal;
                    if (classValue == -1.0) {           //this data points for prediction
                        cepEvent[paramCount - 1] = -1;
                    } else {
                        cepEvent[paramCount - 1] = classValue;
                    }
                    for (int i = 0; i < paramCount; i++) {
                        evt = attributeExpressionExecutors[i + paramPosition].execute(complexEvent);
                        cepEvent[i] = (double) evt;
                    }

                    Object[] outputData ;
                    outputData = streamingRegression.regress(cepEvent);
                    if (outputData == null) {
                        streamEventChunk.remove();
                    } else {
                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    }

                } else if (complexEvent.getType() == ComplexEvent.Type.TIMER) {
                    lastScheduledTimestamp = lastScheduledTimestamp + TIMER_DURATION;
                    scheduler.notifyAt(lastScheduledTimestamp);

                    Object[] outputData ;
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
