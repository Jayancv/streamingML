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

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.Option;
import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.impl.SimpleComponentFactory;
import org.apache.samoa.topology.impl.SimpleEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StreamingRegressionTaskBuilder {

    private static final String SUPPRESS_STATUS_OUT_MSG = "Suppress the task status output. Normally it is sent to stderr.";
    private static final String SUPPRESS_RESULT_OUT_MSG = "Suppress the task result output. Normally it is sent to stdout.";
    private static final String STATUS_UPDATE_FREQ_MSG = "Wait time in milliseconds between status updates.";

    private static final Logger logger = LoggerFactory.getLogger(StreamingRegressionTaskBuilder.class);

    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<Vector> samoaPredictions;

    public int maxInstances = 1000000;
    public int batchSize = 1000;
    public int numAttr = 2;
    public int parallel = 1;

    public StreamingRegressionTaskBuilder(int maxInstance, int batchSize, int numAtts, ConcurrentLinkedQueue<double[]>
            cepEvents, ConcurrentLinkedQueue<Vector> data, int par) {
        this.cepEvents = cepEvents;
        this.maxInstances = maxInstance;
        this.batchSize = batchSize;
        this.numAttr = numAtts;
        this.samoaPredictions = data;
        this.parallel = par;
    }

    public void initTask() {

        String query = "";
        query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.regression.StreamingRegressionTask -f " +
                batchSize + " -i " + maxInstances + " -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa." +
                "regression.StreamingRegressionStream -A " + numAttr + " ) " +
                "-l  (org.apache.samoa.learners.classifiers.rules.HorizontalAMRulesRegressor -r 9 -p " + parallel + ")";

        logger.info("QUERY: " + query);
        String args[] = {query};
        this.initRegressionTask(args);
    }

    private void initRegressionTask(String[] args) {

        /// No usage
        FlagOption suppressStatusOutOpt = new FlagOption("suppressStatusOut", 'S', SUPPRESS_STATUS_OUT_MSG);
        FlagOption suppressResultOutOpt = new FlagOption("suppressResultOut", 'R', SUPPRESS_RESULT_OUT_MSG);
        IntOption statusUpdateFreqOpt = new IntOption("statusUpdateFrequency", 'F', STATUS_UPDATE_FREQ_MSG, 1000, 0,
                Integer.MAX_VALUE);

        Option[] extraOptions = new Option[]{suppressStatusOutOpt, suppressResultOutOpt, statusUpdateFreqOpt};
        ///

        StringBuilder cliString = new StringBuilder();
        for (String arg : args) {
            cliString.append(" ").append(arg);
        }
        logger.debug("Command line string = {}", cliString.toString());

        Task task;
        try {
            task = ClassOption.cliStringToObject(cliString.toString(), Task.class, extraOptions);
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Fail to initialize the task : ", e);
        }

        if (task instanceof StreamingRegressionTask) {
            StreamingRegressionTask t = (StreamingRegressionTask) task;
            t.setCepEvents(this.cepEvents);
            t.setSamoaData(this.samoaPredictions);
        } else {
            throw new ExecutionPlanRuntimeException("Check Task: Not a StreamingRegressionTask");

        }
        logger.info("Successfully Convert the Task into StreamingRegressionTask");
        task.setFactory(new SimpleComponentFactory());
        logger.info("Successfully Initialized Component Factory");
        task.init();
        logger.info("Successfully Initiated the StreamingRegressionTask");
        SimpleEngine.submitTopology(task.getTopology());
        logger.info("Samoa Simple Engine Started");

    }
}
