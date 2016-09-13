package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import org.apache.samoa.instances.*;
import org.apache.samoa.moa.core.DataPoint;
import org.apache.samoa.moa.core.Example;
import org.apache.samoa.moa.core.InstanceExample;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.clustering.ClusteringStream;
import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Created by wso2123 on 9/9/16.
 */
public class ClassificationStream1  extends ClusteringStream{

    public ConcurrentLinkedQueue<double[]> cepEvents;
    private static final Logger logger = LoggerFactory.getLogger(ClassificationStream.class);
    protected InstancesHeader streamHeader;
    private int numGeneratedInstances;
    private int nextEventCounter;
    LinkedList<DataPoint> points = new LinkedList<DataPoint>();
    double [] values; //Cep Event
    private int numAttributes=5;

   //public IntOption numClusterOption = new IntOption("numCluster", 'K',
     //       "The average number of centroids in the model.", 5, 1, Integer.MAX_VALUE);

    protected void prepareForUseImpl(TaskMonitor taskMonitor, ObjectRepository objectRepository) {
        taskMonitor.setCurrentActivity("Preparing random RBF...", -1.0);
        this.numAttributes =5;
        logger.info("Number of Attributes in the Stream : "+this.numAttributes);
        generateHeader();
        restart();
        //logger.info("Succefully Prepare MyClusteringStream for Implementation");
        values = new double[numAttributes];

        for(int i=0;i<numAttributes;i++){
            values[i]=0;
        }
    }

    @Override
    public InstancesHeader getHeader() {
        return streamHeader;
    }

    @Override
    public long estimatedRemainingInstances() {
        return -1L ;
    }

    @Override
    public boolean hasMoreInstances() {
        return true;
    }

    public Example<Instance> nextInstance() {
        //logger.info(("Next event"));
        //int numGeneratedInstances = 0;
        if(numGeneratedInstances == 0){
            logger.info("Sending First Samoa Instance.....");
            numGeneratedInstances++;
            //double[] values = this.values;
            double[] values_new = new double[5]; // +1
            int clusterChoice = -1;
            while(cepEvents == null);
            while (cepEvents.isEmpty()) ;
            double[] values = cepEvents.poll();
            System.arraycopy(values, 0, values_new, 0, values.length);
            Instance inst = new DenseInstance(1.0, values_new);
            inst.setDataset(getHeader());
            return new InstanceExample(inst);

        }else {
            numGeneratedInstances++;
            // logger.info("Sending Samoa Instance :"+numGeneratedInstances);
            double[] values_new = new double[5]; // +1
            //logger.info("I am here");

            //while(cepEvents == null);
            while (cepEvents.isEmpty()) ;
            //logger.info("Cep Events Not Empty");
            double[] values = cepEvents.poll();
            int clusterChoice = -1;
            System.arraycopy(values, 0, values_new, 0, values.length);
            Instance inst = new DenseInstance(1.0, values_new);
            inst.setDataset(getHeader());
            return new InstanceExample(inst);
        }
    }

    @Override
    public boolean isRestartable() {
        return true;
    }


    @Override
    public void restart() {
        numGeneratedInstances =0;

    }


    @Override
    public void getDescription(StringBuilder stringBuilder, int i) {

    }

    private void addInstance(Instance instance) {
        DataPoint point = new DataPoint(instance, numGeneratedInstances);
        points.add(point);

    }

    protected void generateHeader() {
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
        for (int i = 0; i < 5; i++) {
            attributes.add(new Attribute("att" + (i + 1)));
        }

//        ArrayList<String> classLabels = new ArrayList<String>();
//        for (int i = 0; i < this.numClusterOption.getValue(); i++) {
//            classLabels.add("class" + (i + 1));
//        }

        //attributes.add(new Attribute("class", classLabels));
        streamHeader = new InstancesHeader(new Instances(getCLICreationString(InstanceStream.class), attributes, 0));
        streamHeader.setClassIndex(streamHeader.numAttributes() - 1);
    }

    public void setCepEvents(ConcurrentLinkedQueue<double[]> cepEvents) {
        this.cepEvents = cepEvents;
    }


}
