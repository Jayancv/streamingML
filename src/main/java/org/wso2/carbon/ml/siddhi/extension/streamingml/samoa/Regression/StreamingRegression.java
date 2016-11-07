package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by wso2123 on 10/14/16.
 */
public class StreamingRegression extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(StreamingRegression.class);

    private int maxInstance = 100000;
    private int batchSize = 500;            //Output display interval
    private int numAttributes = 0;
    private int paralle = 1;
    private int bagging = 1;
    public int numEventsReceived = 0;

    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<Vector> samoaPredictions;

    public StreamingRegressionTaskBuilder regressionTask;


    public StreamingRegression(int maxint, int batchSize, int paramCount, int parallelism, int bagging) {

        this.maxInstance = maxint;
        this.numAttributes = paramCount;
        this.batchSize = batchSize;
        this.paralle = parallelism;
        this.bagging = bagging;

        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaPredictions = new ConcurrentLinkedQueue<Vector>();
        try {
            this.regressionTask = new StreamingRegressionTaskBuilder(this.maxInstance, this.batchSize, this.numAttributes, this.cepEvents, this.samoaPredictions, this.paralle, this.bagging);
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        logger.info("Successfully Initiated the Streaming StreamingRegression Topology");
    }

    public void run() {
        regressionTask.initTask(maxInstance, batchSize, numAttributes, paralle, bagging);
    }

    public Object[] regress(double[] cepEvent) {
        numEventsReceived++;
        cepEvents.add(cepEvent);
        Object[] output;

        if (!samoaPredictions.isEmpty()) {                     // poll predicted events from prediction queue 
            String str = "";
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

    public Object[] regress() {                                       // for cep timeEvents
        numEventsReceived++;
        Object[] output;
        if (!samoaPredictions.isEmpty()) {
            String str = "";
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
