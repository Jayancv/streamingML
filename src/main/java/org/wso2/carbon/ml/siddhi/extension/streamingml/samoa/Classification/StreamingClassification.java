package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification;

import org.apache.spark.sql.catalyst.expressions.In;
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
    private static final Logger logger = LoggerFactory.getLogger(StreamingClassification.class);

    private int maxInstance = Integer.MAX_VALUE;
    private int batchSize = 1000;                       //Output display interval
    private int numClasses = 2;
    private int numAttributes = 0;
    private int numNominals = 0;
    private int paralle = 1;
    private int bagging = 1;
    private String nominalAttVals = "";
    public int numEventsReceived = 0;

    public ConcurrentLinkedQueue<double[]> cepEvents;                                    //Cep events
    public ConcurrentLinkedQueue<Vector> samoaClassifiers;                               // Output prediction data

    public StreamingClassificationTaskBuilder classificationTask;

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
        cepEvents.add(eventData);
        Object[] output;

        if (!samoaClassifiers.isEmpty()) {
            String str = "";
            output = new Object[numAttributes];
            Vector prediction = samoaClassifiers.poll();
            for (int i = 0; i < prediction.size(); i++) {
                output[i] = prediction.get(i);
            }
        } else {
            output = null;
        }
        return output;
    }


    public Object[] getClassify() {
        Object[] output;
        if (!samoaClassifiers.isEmpty()) {
            String str = "";
            output = new Object[numAttributes];
            Vector prediction = samoaClassifiers.poll();
            for (int i = 0; i < prediction.size(); i++) {
                output[i] = prediction.get(i);
            }
        } else {
            output = null;
        }
        return output;
    }
}
