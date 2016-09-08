package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;
import org.apache.samoa.learners.classifiers.SingleClassifier;
import org.apache.samoa.moa.classifiers.AbstractClassifier;
import org.apache.samoa.moa.classifiers.Classifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by wso2123 on 8/30/16.
 */
public class Classification extends Thread {

    private int maxInstance=100000;
    private int step =1 ;                ///Output display interval
    private int numClasses=2;
    private int numAttributes = 0;
    private int batchSize=100;
    public int numEventsReceived=0;
    private List<String> eventsMem=null;

    private boolean isBuiltModel;
    private MODEL_TYPE type;
    public enum MODEL_TYPE {BATCH_PROCESS, MOVING_WINDOW,TIME_BASED }


    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<AbstractClassifier>samoaClassifiers;

    public ClassificationTaskBuilder classificationTask;

    private static final Logger logger = LoggerFactory.getLogger(Classification.class);


    public Classification(int maxInstance,int steps, int classses, int paraCount){

        this.maxInstance = maxInstance;
        this.step =steps;
        this.numClasses = classses;
        this.numAttributes=paraCount;

        this.isBuiltModel = false;
        type= MODEL_TYPE.BATCH_PROCESS;


        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaClassifiers = new  ConcurrentLinkedQueue<AbstractClassifier>();
        this.maxInstance = 1000000;
        try {
            this.classificationTask = new ClassificationTaskBuilder(this.numClasses,this.cepEvents, this.samoaClassifiers, this.maxInstance);
        }catch(Exception e){
            System.out.println(e.toString());
        }
        logger.info("Successfully Initiated the Streaming Classification Topology");

    }

    public void run(){
        classificationTask.initTask( numAttributes,  numClasses,  maxInstance);

    }

    public Object[] classify(double[] eventData) {
        numEventsReceived++;
        //logger.info("CEP Event Received : "+numEventsReceived);
        cepEvents.add(eventData);
        Object[] output;
        if(!samoaClassifiers.isEmpty()){
            logger.info("Update the Model");
            output = new Object[numClasses +1];
            output[0] = 0.0;
            //System.out.println("++++ We got a hit ++++");
            AbstractClassifier classifiers = samoaClassifiers.poll();
           //int dim = classifier.get();

            for(int i=0;i<numClasses;i++){
                Classifier[] classifier=classifiers.getSubClassifiers();
                String a = classifier.toString();
            }
        }else{
            output=null;
        }
        return output;
    }

}
