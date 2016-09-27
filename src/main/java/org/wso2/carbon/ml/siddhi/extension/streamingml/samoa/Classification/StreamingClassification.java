package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by wso2123 on 8/30/16.
 */
public class StreamingClassification extends Thread {

    private int maxInstance = 100000;
    private int step = 1;
    private int numClasses = 2;
    private int numAttributes = 0;
    private int batchSize = 500;            //Output display interval
    public int numEventsReceived = 0;
    private List<String> eventsMem = null;

    private boolean isBuiltModel;
    private MODEL_TYPE type;

    public enum MODEL_TYPE {BATCH_PROCESS, MOVING_WINDOW, TIME_BASED}


    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<Vector> samoaClassifiers;

    public StreamingClassificationTaskBuilder classificationTask;

    private static final Logger logger = LoggerFactory.getLogger(StreamingClassification.class);


    public StreamingClassification(int maxInstance, int batchSize, int classes, int paraCount) {

        this.maxInstance = maxInstance;
        this.numClasses = classes;
        this.numAttributes = paraCount;
        this.batchSize = batchSize;

        this.isBuiltModel = false;
        type = MODEL_TYPE.BATCH_PROCESS;

        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaClassifiers = new ConcurrentLinkedQueue<Vector>();
        try {
            this.classificationTask = new StreamingClassificationTaskBuilder(this.numClasses, this.cepEvents, this.samoaClassifiers, this.numAttributes, this.maxInstance, this.batchSize);
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        logger.info("Successfully Initiated the Streaming StreamingClassification Topology");

    }

    public void run() {
        classificationTask.initTask(numAttributes, numClasses, maxInstance, batchSize);

    }

    public Object[] classify(double[] eventData) {
        numEventsReceived++;
        //logger.info("CEP Event Received : "+numEventsReceived);
        cepEvents.add(eventData);
        Object[] output;
        if (!samoaClassifiers.isEmpty()) {
//            logger.info("Update the Model");
            output = new Object[4];
            output[0] = 0.0;
            Vector classifiers = samoaClassifiers.poll();

            for (int i = 0; i < 4; i++) {
                output[i] = classifiers.get(i + 1);
                System.out.println(classifiers.get(i + 1));
            }
        } else {
            output = null;
        }
        return output;
    }

}
