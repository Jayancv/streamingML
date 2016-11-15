
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

import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.clustering.StreamingClustering;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.condition.In;

import java.util.ArrayList;
import java.util.List;

public class StreamingClusteringWithSamoaStreamProcessor extends StreamProcessor {

    private int paramCount = 0;                                         // Number of x variables +1
    private int paramPosition = 0;
    private int maxInstance = Integer.MAX_VALUE;
    private int numClusters = 1;
    private StreamingClustering streamingClusteringWithSamoa = null;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors
            , ExecutionPlanContext executionPlanContext) {
        paramCount = attributeExpressionLength;
        int PARAM_WIDTH = 2;

        if (attributeExpressionExecutors.length > PARAM_WIDTH) {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                maxInstance = ((Integer) attributeExpressionExecutors[0].execute(null));
                if (maxInstance == -1) {
                    maxInstance = Integer.MAX_VALUE;
                }
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the first argument, " +
                        "required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[0].getReturnType().toString());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                numClusters = ((Integer) attributeExpressionExecutors[1].execute(null));
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the fourth argument, " +
                        "required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[1].getReturnType().toString());
            }


        } else {
            throw new ExecutionPlanValidationException("Invalid no of arguments, required more than " + (PARAM_WIDTH) +
                    ", but found " + attributeExpressionExecutors.length + "(Attribute count should be equal or" +
                    " grater than 1) : streamingClusteringSamoa(numMaxEvents,miniBatch, numClusters, " +
                    " ,attribute_set)");
        }

        paramCount = paramCount - PARAM_WIDTH;
        paramPosition = PARAM_WIDTH;
        streamingClusteringWithSamoa = new StreamingClustering(paramCount, numClusters);
        try {
            Thread.sleep(1000);
        } catch (Exception e) {

        }
        new Thread(streamingClusteringWithSamoa).start();
        // Add attributes for standard error and all beta values
        String betaVal;
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(numClusters + 1);
        attributes.add(new Attribute("stderr", Attribute.Type.DOUBLE));

        for (int itr = 0; itr < numClusters; itr++) {
            betaVal = "center" + itr;
            attributes.add(new Attribute(betaVal, Attribute.Type.STRING));
        }

        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner
            streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();

                double[] cepEvent = new double[attributeExpressionLength - paramPosition];
                Double value;
                for (int i = paramPosition; i < attributeExpressionLength; i++) {
                    cepEvent[i - paramPosition] = (double) attributeExpressionExecutors[i].execute(complexEvent);
                }

                Object[] outputData = null;
                outputData = streamingClusteringWithSamoa.cluster(cepEvent);

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

}
