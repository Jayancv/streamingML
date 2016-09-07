package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.classifiers.AbstractClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by wso2123 on 8/30/16.
 */
public class ClassificationEvaluationProcessor implements Processor {

    private static final long serialVersionUID = -6043613438148776446L;
    private int processorId;
    private static final Logger logger = LoggerFactory.getLogger(ClassificationEvaluationProcessor.class);

    String evalPoint;
    //public LinkedList<Clustering>samoaClusters;
    //public ConcurrentLinkedQueue<double[]> cepEvents;
   // public ConcurrentLinkedQueue<AbstractClassifier> samoaClassifiers;
    public int numClasses=0;
    private ConcurrentLinkedQueue<AbstractClassifier> samoaClassifiers;

    public ClassificationEvaluationProcessor(String s) {
        this.evalPoint=s;
    }

    @Override
    public boolean process(ContentEvent event) {

        if (event instanceof InstanceContentEvent) {
            logger.info(event.getKey()+""+evalPoint+"ClusteringContentEvent");
            InstanceContentEvent e= (InstanceContentEvent) event;
            Instance inst = e.getInstance();

            int numAttributes=inst.numAttributes();
        }

        else if(event instanceof ResultContentEvent){
            logger.info(event.getKey()+" "+evalPoint+" ClusteringResultContentEvent "+numClasses);
            ResultContentEvent resultEvent = (ResultContentEvent) event;

           // Clustering clustering=resultEvent.getClustering();

           // Clustering kmeansClustering = WithKmeans.kMeans_rand(numClusters,clustering);
           // logger.info("Kmean Clusters: "+kmeansClustering.size()+" with dimention of : "+kmeansClustering.dimension());
            //Adding samoa Clusters into my class
            //samoaClusters.add(kmeansClustering);

            //int numClusters = clustering.size();
            //logger.info("Number of Kernal Clusters : "+numClusters+" Number of KMeans Clusters :"+kmeansClustering.size());
         //AbstractClassifier classifier=resultEvent.ge;

        }

//        else if(event instanceof ClusteringEvaluationContentEvent){
//            logger.info(event.getKey()+""+evalPoint+"ClusteringEvaluationContentEvent\n");
//        }
//        else{
//            logger.info(event.getKey()+""+evalPoint+"ContentEvent\n");
//        }

        return true;
    }

    @Override
    public void onCreate(int i) {

    }

    @Override
    public Processor newProcessor(Processor processor) {
        return null;
    }

   

    public void setNumClasses(int numClasses) {
        this.numClasses = numClasses;
    }

    public void setSamoaClassifiers(ConcurrentLinkedQueue<AbstractClassifier> samoaClassifiers) {
        this.samoaClassifiers = samoaClassifiers;
    }
}
