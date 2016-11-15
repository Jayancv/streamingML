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
package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.regression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StreamingRegression extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(StreamingRegression.class);

    private int maxInstance = 100000;
    private int batchSize = 500;            //Output display interval
    private int numAttributes = 0;
    private int paralle = 1;
    public int numEventsReceived = 0;

    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<Vector> samoaPredictions;

    public StreamingRegressionTaskBuilder regressionTask;

    public StreamingRegression(int maxint, int batchSize, int paramCount, int parallelism) {

        this.maxInstance = maxint;
        this.numAttributes = paramCount;
        this.batchSize = batchSize;
        this.paralle = parallelism;

        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaPredictions = new ConcurrentLinkedQueue<Vector>();
        try {
            this.regressionTask = new StreamingRegressionTaskBuilder(this.maxInstance, this.batchSize,
                    this.numAttributes, this.cepEvents, this.samoaPredictions, this.paralle);
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Fail to Initiate the Streaming Regression.", e);
        }
    }

    public void run() {
        regressionTask.initTask();
    }

    public Object[] regress(double[] cepEvent) {
        numEventsReceived++;
        cepEvents.add(cepEvent);
        Object[] output;

        if (!samoaPredictions.isEmpty()) {                     // poll predicted events from prediction queue
            output = new Object[numAttributes];
            Vector prediction = samoaPredictions.poll();
            for (int i = 0; i < prediction.size(); i++) {
                output[i] = prediction.get(i);
            }
            System.out.println(prediction);
        } else {
            output = null;
        }
        return output;
    }

    public Object[] regress() {                                       // for cep timerEvents
        numEventsReceived++;
        Object[] output;
        if (!samoaPredictions.isEmpty()) {
            output = new Object[numAttributes];
            Vector prediction = samoaPredictions.poll();
            for (int i = 0; i < prediction.size(); i++) {
                output[i] = prediction.get(i);
            }
        } else {
            output = null;
        }
        return output;
    }
}
