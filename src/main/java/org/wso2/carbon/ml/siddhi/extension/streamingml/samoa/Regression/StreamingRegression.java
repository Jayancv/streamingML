package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by wso2123 on 10/14/16.
 */
public class StreamingRegression extends Thread {

    private int maxInstance = 100000;
    private int batchSize = 500;            //Output display interval
    private int numAttributes = 0;
    private int paralle = 1;
    private int bagging = 1;
    public int numEventsReceived = 0;

    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<Vector> samoaData;

    public StreamingRegressionTaskBuilder regressingTask;

    private static final Logger logger = LoggerFactory.getLogger(StreamingRegression.class);

    public StreamingRegression(int maxint, int paramCount, int batchSize, int par, int bagging) {

        this.maxInstance = maxint;
        this.numAttributes = paramCount;
        this.batchSize = batchSize;
        this.paralle = par;
        this.bagging = bagging;

        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaData = new ConcurrentLinkedQueue<Vector>();
        try {
            this.regressingTask = new StreamingRegressionTaskBuilder(this.maxInstance, this.batchSize, this.numAttributes, this.cepEvents, this.samoaData, this.paralle, this.bagging);
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        logger.info("Successfully Initiated the Streaming StreamingClassification Topology");

    }

    public static Object[] regress(double[] cepEvent) {
        return null;
    }


    public void run() {
        regressingTask.initTask(maxInstance, batchSize, numAttributes, paralle, bagging);

    }

    public Object[] classify(double[] eventData) {
        numEventsReceived++;
        //logger.info("CEP Event Received : "+numEventsReceived);
        cepEvents.add(eventData);
        int numObjects = 5;
        Object[] output;

        if (!samoaData.isEmpty()) {
//            logger.info("Update the Model");
            String str = "";
            output = new Object[5];
            output[0] = 0.0;
            Vector data = samoaData.poll();
            int l = 0;
//            for (int i = 0; i < 9; i++) {
//
//                if (i < 4) {
//                    output[i] = classifiers.get(l + 1);
//                    String inter = classifiers.get(l + 1).toString();
//                    String sss = inter.substring(inter.lastIndexOf("=") + 1);
//                    String ww = String.valueOf(sss).replace(",", "");
//                    str = str + "," + ww;
//
//                    //System.out.println(regressionData.get(l + 1));
//                    l++;
//                } else if (8 > i) {
//                    String[] o = new String[numClasses];
//                    for (int j = 0; j < numClasses; j++) {
//                        o[j] = classifiers.get(l + 1).toString();
//
//                        String inter = classifiers.get(l + 1).toString();
//                        String sss = inter.substring(inter.lastIndexOf("=") + 1);
//                        String ww = String.valueOf(sss).replace(",", "");
//                        str = str + "," + ww;
//
//                        l++;
//
//                    }
//                    String b = Arrays.toString(o);
//                    output[i] = b.toString();
//
//
//                    //System.out.println(b);
//                } else {
//                    String[] o = new String[batchSize];
//                    for (int j = 0; j < batchSize; j++) {
//                        o[j] = classifiers.get(l + 1).toString();
//                        String inter = classifiers.get(l + 1).toString();
//                        String sss = inter.substring(inter.lastIndexOf("=") + 1);
//                        String ww = String.valueOf(sss).replace(",", "");
//                        str = str + "," + ww;
//
//                        l++;
//
//                    }
//                    String b = Arrays.toString(o);
//                    output[i] = b.toString();
//                }
//
//            }
//            System.out.println(str.substring(2));
        } else {
            output = null;
        }

        return output;
    }


}
