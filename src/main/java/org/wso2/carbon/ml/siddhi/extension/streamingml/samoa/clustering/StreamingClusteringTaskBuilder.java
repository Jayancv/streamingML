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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.clustering;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.Option;

import org.apache.samoa.moa.cluster.Clustering;
import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.impl.SimpleComponentFactory;
import org.apache.samoa.topology.impl.SimpleEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.concurrent.ConcurrentLinkedQueue;

public class StreamingClusteringTaskBuilder {


    // It seems that the 3 extra options are not used.
    private static final String SUPPRESS_STATUS_OUT_MSG = "Suppress the task status output.Normally it is sent to stderr.";
    private static final String SUPPRESS_RESULT_OUT_MSG = "Suppress the task result output.Normally it is sent to stdout.";
    private static final String STATUS_UPDATE_FREQ_MSG = "Wait time in milliseconds between status updates.";
    private static final Logger logger = LoggerFactory.getLogger(StreamingClusteringTaskBuilder.class);

    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<Clustering> samoaClusters;
    public int numClusters = 0;
    public int maxNumEvents = 100000;

    public StreamingClusteringTaskBuilder(int numClusters, ConcurrentLinkedQueue<double[]> cepEvents,
                                          ConcurrentLinkedQueue<Clustering> samoaClusters, int maxNumEvents) {
        this.numClusters = numClusters;
        this.cepEvents = cepEvents;
        this.samoaClusters = samoaClusters;
        this.maxNumEvents = maxNumEvents;
    }

    public void initTask(int numAttributes, int numClusters, int maxNumEvents) {
        String query = "";
        query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.clustering.StreamingClusteringTask -f 1000" +
                " -i " + maxNumEvents + " -s  (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa." +
                "clustering.StreamingClusteringStream -K " + numClusters + " -a " + numAttributes + ") -l" +
                " (org.apache.samoa.learners.clusterers.simple.DistributedClusterer " +
                "-l (org.apache.samoa.learners.clusterers.ClustreamClustererAdapter " +
                "-l (org.apache.samoa.moa.clusterers.clustream.WithKmeans  -m 100 -k " + numClusters + ")))";
        logger.info("QUERY: " + query);
        String args[] = {query};
        this.initClusteringTask(args);
    }

    public void initClusteringTask(String[] args) {
        FlagOption suppressStatusOutOpt = new FlagOption("suppressStatusOut", 'S', SUPPRESS_STATUS_OUT_MSG);
        FlagOption suppressResultOutOpt = new FlagOption("suppressResultOut", 'R', SUPPRESS_RESULT_OUT_MSG);
        IntOption statusUpdateFreqOpt = new IntOption("statusUpdateFrequency", 'F', STATUS_UPDATE_FREQ_MSG, 1000, 0,
                Integer.MAX_VALUE);

        Option[] extraOptions = new Option[]{suppressStatusOutOpt, suppressResultOutOpt, statusUpdateFreqOpt};
        StringBuilder cliString = new StringBuilder();
        for (String arg : args) {
            cliString.append(" ").append(arg);
        }

        Task task;
        try {
            task = ClassOption.cliStringToObject(cliString.toString(), Task.class, extraOptions);
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Fail to initialize the task", e);
        }
        if (task instanceof StreamingClusteringTask) {
            StreamingClusteringTask t = (StreamingClusteringTask) task;
            t.setCepEvents(this.cepEvents);
            t.setSamoaClusters(this.samoaClusters);
            t.setNumClusters(this.numClusters);

        } else {
            throw new ExecutionPlanRuntimeException("Check Task: Not a StreamingClassificationTask");
        }

        task.setFactory(new SimpleComponentFactory());
        task.init();
        logger.info("Successfully Initiated the StreamingClusteringTask");
        SimpleEngine.submitTopology(task.getTopology());
    }
}
