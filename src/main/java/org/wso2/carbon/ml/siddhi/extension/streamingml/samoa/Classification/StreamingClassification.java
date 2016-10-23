package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.combinator.testing.Str;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by wso2123 on 8/30/16.
 */
public class StreamingClassification extends Thread {

    private int maxInstance = 100000;
    private int batchSize = 500;            //Output display interval
    private int numClasses = 2;
    private int numAttributes = 0;
    private int numNominals = 0;
    private int paralle = 1;
    private int bagging = 1;
    private String nominalAttVals = "";
    public int numEventsReceived = 0;


    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<Vector> samoaClassifiers;

    public StreamingClassificationTaskBuilder classificationTask;

    private static final Logger logger = LoggerFactory.getLogger(StreamingClassification.class);


    public StreamingClassification(int maxInstance, int batchSize, int classes, int paraCount, int nominals, String str, int par, int bagging) {

        this.maxInstance = maxInstance;
        this.numClasses = classes;
        this.numAttributes = paraCount;
        this.numNominals = nominals;
        this.batchSize = batchSize;
        this.nominalAttVals = str;
        this.paralle = par;
        this.bagging = bagging;

        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaClassifiers = new ConcurrentLinkedQueue<Vector>();
        try {
            this.classificationTask = new StreamingClassificationTaskBuilder(this.maxInstance, this.batchSize, this.numClasses, this.numAttributes, this.numNominals, this.cepEvents, this.samoaClassifiers, this.paralle, this.bagging);
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        logger.info("Successfully Initiated the Streaming StreamingClassification Topology");

    }

    public void run() {
        classificationTask.initTask(maxInstance, batchSize, numClasses, numAttributes, numNominals, nominalAttVals, paralle, bagging);

    }

    public Object[] classify(double[] eventData) {
        numEventsReceived++;
        //logger.info("CEP Event Received : "+numEventsReceived);
        cepEvents.add(eventData);
        int numObjects = 4 + (numClasses * 4) + 1;
        Object[] output;

        if (!samoaClassifiers.isEmpty()) {
//            logger.info("Update the Model");
            String str = "";
            output = new Object[9];
            output[0] = 0.0;
            Vector classifiers = samoaClassifiers.poll();
            int l = 0;
            for (int i = 0; i < 9; i++) {

                if (i < 4) {
                    output[i] = classifiers.get(l + 1);
                    String inter = classifiers.get(l + 1).toString();
                    String sss = inter.substring(inter.lastIndexOf("=") + 1);
                    String ww = String.valueOf(sss).replace(",", "");
                    str = str + "," + ww;

                    //System.out.println(regressionData.get(l + 1));
                    l++;
                } else if (8 > i) {
                    String[] o = new String[numClasses];
                    for (int j = 0; j < numClasses; j++) {
                        o[j] = classifiers.get(l + 1).toString();

                        String inter = classifiers.get(l + 1).toString();
                        String sss = inter.substring(inter.lastIndexOf("=") + 1);
                        String ww = String.valueOf(sss).replace(",", "");
                        str = str + "," + ww;

                        l++;

                    }
                    String b = Arrays.toString(o);
                    output[i] = b.toString();


                    //System.out.println(b);
                } else {
                    String[] o = new String[batchSize];
                    for (int j = 0; j < batchSize; j++) {
                        o[j] = classifiers.get(l + 1).toString();
                        String inter = classifiers.get(l + 1).toString();
                        String sss = inter.substring(inter.lastIndexOf("=") + 1);
                        String ww = String.valueOf(sss).replace(",", "");
                        str = str + "," + ww;

                        l++;

                    }
                    String b = Arrays.toString(o);
                    output[i] = b.toString();
                }

            }
            System.out.println(str.substring(2));
        } else {
            output = null;
        }

        return output;
    }

}
