package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import org.apache.samoa.moa.classifiers.AbstractClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by wso2123 on 8/30/16.
 */
public class Classification extends Thread {

    private int maxInstance=0;
    private int step =0 ;                ///Output display interval
    private int numClasses=1;

    public int numEventsReceived=0;
    private List<String> eventsMem=null;

    private boolean isBuiltModel;
    private Classification.MODEL_TYPE type;
    public enum MODEL_TYPE {BATCH_PROCESS, MOVING_WINDOW,TIME_BASED }

    //public  LinkedList<double[]> cepEvents;
    // public LinkedList<Clustering>samoaClusters;

    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<AbstractClassifier>samoaClassifiers;
    public ClassificationTaskBuilder classificationTask;
    private static final Logger logger = LoggerFactory.getLogger(Classification.class);


    public Classification(){

    }

    public void run(){


    }

    public Object[] classify(double[] eventData) {
        System.out.println("data passing");

        return null ;
    }

}
