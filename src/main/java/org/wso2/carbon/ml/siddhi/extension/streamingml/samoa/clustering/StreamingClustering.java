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

import org.apache.samoa.moa.cluster.Cluster;
import org.apache.samoa.moa.cluster.Clustering;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;


public class StreamingClustering extends Thread{

    private int paramCount = 0;
    private int numAttributes=0;
    private int batchSize = 10;
    private int numClusters=1;

    public ConcurrentLinkedQueue<double[]>cepEvents;
    public ConcurrentLinkedQueue<Clustering>samoaClusters;
    public int maxNumEvents=Integer.MAX_VALUE;
    public int numEventsReceived=0;

    public StreamingClusteringTaskBuilder clusteringTask;
    private static final Logger logger = LoggerFactory.getLogger(StreamingClustering.class);

    public StreamingClustering(int paramCount, int batchSize,  int numClusters){
        this.paramCount =paramCount;
        this.numAttributes = paramCount;
        this.batchSize = batchSize;
        this.numClusters = numClusters;


        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaClusters = new  ConcurrentLinkedQueue<Clustering>();

        try {
            this.clusteringTask = new StreamingClusteringTaskBuilder(this.numClusters,this.cepEvents, this.samoaClusters, this.maxNumEvents);
        }catch(Exception e){
            System.out.println(e.toString());
        }
        logger.info("Successfully Initiated the Streaming Clustering Topology");
    }

    public void run()
    {
        this.clusteringTask.initTask(paramCount,numClusters,batchSize,maxNumEvents);
    }

    public Object[] cluster(double[] eventData) {
        numEventsReceived++;
        cepEvents.add(eventData);

        Object[] output;
        if(!samoaClusters.isEmpty()){

            output = new Object[numClusters +1];
            output[0] = 0.0;
            Clustering clusters = samoaClusters.poll();
            for (int i=0;i<numClusters;i++){
                Cluster cluster= clusters.get(i);
                String centerStr="";
                double [] center=cluster.getCenter();
                centerStr += center[0];
                for(int j=1;j<numAttributes;j++){
                    centerStr += (","+center[j]);
                }
                output[i+1]= centerStr;
            }

        }else{
            output=null;
        }
        return output;
    }



}
