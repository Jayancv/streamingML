package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import org.apache.samoa.instances.*;
import org.apache.samoa.moa.cluster.Cluster;
import org.apache.samoa.moa.core.*;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.clustering.ClusteringStream;
//import org.apache.samoa.streams.generators.RandomTreeGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Created by wso2123 on 8/30/16.
 */
public class ClassificationStream extends ClusteringStream {

//    public ConcurrentLinkedQueue<double[]> cepEvents;
   private static final Logger logger = LoggerFactory.getLogger(ClassificationStream.class);
//    protected InstancesHeader streamHeader;
//    private int numGeneratedInstances;
//    private int nextEventCounter;
//    LinkedList<DataPoint> points = new LinkedList<DataPoint>();
//    double [] values; //Cep Event
   private int numAttributes=5;
//
//    //public IntOption numClusterOption = new IntOption("numCluster", 'K',
//     //       "The average number of centroids in the model.", 5, 1, Integer.MAX_VALUE);
//
//    protected void prepareForUseImpl(TaskMonitor taskMonitor, ObjectRepository objectRepository) {
//        taskMonitor.setCurrentActivity("Preparing random RBF...", -1.0);
//        this.numAttributes =5;
//        logger.info("Number of Attributes in the Stream : "+this.numAttributes);
//        generateHeader();
//        restart();
//        //logger.info("Succefully Prepare MyClusteringStream for Implementation");
//        values = new double[numAttributes];
//
//        for(int i=0;i<numAttributes;i++){
//            values[i]=0;
//        }
//    }
//
//    @Override
//    public InstancesHeader getHeader() {
//        return streamHeader;
//    }
//
//    @Override
//    public long estimatedRemainingInstances() {
//        return -1L ;
//    }
//
//    @Override
//    public boolean hasMoreInstances() {
//        return true;
//    }
//
//    public Example<Instance> nextInstance() {
//        //logger.info(("Next event"));
//        //int numGeneratedInstances = 0;
//        if(numGeneratedInstances == 0){
//            logger.info("Sending First Samoa Instance.....");
//            numGeneratedInstances++;
//            //double[] values = this.values;
//            double[] values_new = new double[5]; // +1
//            int clusterChoice = -1;
//            while(cepEvents == null);
//            while (cepEvents.isEmpty()) ;
//            double[] values = cepEvents.poll();
//            System.arraycopy(values, 0, values_new, 0, values.length);
//            Instance inst = new DenseInstance(1.0, values_new);
//            inst.setDataset(getHeader());
//            return new InstanceExample(inst);
//
//        }else {
//            numGeneratedInstances++;
//            // logger.info("Sending Samoa Instance :"+numGeneratedInstances);
//            double[] values_new = new double[5]; // +1
//            //logger.info("I am here");
//
//            //while(cepEvents == null);
//            while (cepEvents.isEmpty()) ;
//            //logger.info("Cep Events Not Empty");
//            double[] values = cepEvents.poll();
//            int clusterChoice = -1;
//            System.arraycopy(values, 0, values_new, 0, values.length);
//            Instance inst = new DenseInstance(1.0, values_new);
//            inst.setDataset(getHeader());
//            return new InstanceExample(inst);
//        }
//    }
//
//    @Override
//    public boolean isRestartable() {
//        return true;
//    }
//
//
//    @Override
//    public void restart() {
//        numGeneratedInstances =0;
//
//    }
//
//
//    @Override
//    public void getDescription(StringBuilder stringBuilder, int i) {
//
//    }
//
//    private void addInstance(Instance instance) {
//        DataPoint point = new DataPoint(instance, numGeneratedInstances);
//        points.add(point);
//
//    }
//
//    protected void generateHeader() {
//        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
//        for (int i = 0; i < 5; i++) {
//            attributes.add(new Attribute("att" + (i + 1)));
//        }
//
////        ArrayList<String> classLabels = new ArrayList<String>();
////        for (int i = 0; i < this.numClusterOption.getValue(); i++) {
////            classLabels.add("class" + (i + 1));
////        }
//
//        //attributes.add(new Attribute("class", classLabels));
//        streamHeader = new InstancesHeader(new Instances(getCLICreationString(InstanceStream.class), attributes, 0));
//        streamHeader.setClassIndex(streamHeader.numAttributes() - 1);
//    }
//
//    public void setCepEvents(ConcurrentLinkedQueue<double[]> cepEvents) {
//        this.cepEvents = cepEvents;
//    }







    private static final long serialVersionUID = 1L;
    public IntOption treeRandomSeedOption = new IntOption("treeRandomSeed", 'r', "Seed for random generation of tree.", 1);
    public IntOption instanceRandomSeedOption = new IntOption("instanceRandomSeed", 'i', "Seed for random generation of instances.", 1);
    public IntOption numClassesOption = new IntOption("numClasses", 'c', "The number of classes to generate.", 2, 2, 2147483647);
    public IntOption numNominalsOption = new IntOption("numNominals", 'o', "The number of nominal attributes to generate.", 5, 0, 2147483647);
    public IntOption numNumericsOption = new IntOption("numNumerics", 'u', "The number of numeric attributes to generate.", 5, 0, 2147483647);
    public IntOption numValsPerNominalOption = new IntOption("numValsPerNominal", 'v', "The number of values to generate per nominal attribute.", 5, 2, 2147483647);
    public IntOption maxTreeDepthOption = new IntOption("maxTreeDepth", 'd', "The maximum depth of the tree concept.", 5, 0, 2147483647);
    public IntOption firstLeafLevelOption = new IntOption("firstLeafLevel", 'l', "The first level of the tree above maxTreeDepth that can have leaves.", 3, 0, 2147483647);
    public FloatOption leafFractionOption = new FloatOption("leafFraction", 'f', "The fraction of leaves per level from firstLeafLevel onwards.", 0.15D, 0.0D, 1.0D);
    protected ClassificationStream.Node treeRoot;
    protected InstancesHeader streamHeader;
    protected Random instanceRandom;


    private ConcurrentLinkedQueue<double[]> cepEvents;

    public ClassificationStream() {
    }

    public String getPurposeString() {
        return "Generates a stream based on a randomly generated tree.";
    }

    public void prepareForUseImpl(TaskMonitor monitor, ObjectRepository repository) {
        monitor.setCurrentActivity("Preparing random tree...", -1.0D);
        this.generateHeader();
        this.generateRandomTree();
        this.restart();
    }

    public long estimatedRemainingInstances() {
        return -1L;
    }

    public boolean isRestartable() {
        return true;
    }

    public void restart() {
        this.instanceRandom = new Random((long)this.instanceRandomSeedOption.getValue());
    }

    public InstancesHeader getHeader() {
        return this.streamHeader;
    }

    public boolean hasMoreInstances() {
        return true;
    }

    public InstanceExample nextInstance() {
        double[] attVals = new double[this.numNominalsOption.getValue() + this.numNumericsOption.getValue()];
        InstancesHeader header = this.getHeader();
        DenseInstance inst = new DenseInstance((double)header.numAttributes());

        for(int i = 5; i <10; ++i) {
            attVals[i] = i < this.numNominalsOption.getValue()?(double)this.instanceRandom.nextInt(this.numValsPerNominalOption.getValue()):this.instanceRandom.nextDouble();
           // attVals[i] = i < this.numNominalsOption.getValue()?(double)this.instanceRandom.nextInt(this.numValsPerNominalOption.getValue()):cepEvents.poll()[i-5];
          //attVals[i]=cepEvents.poll()[i];
            inst.setValue(i, attVals[i]);
        }

        inst.setDataset(header);
        inst.setClassValue((double)this.classifyInstance(this.treeRoot, attVals));
        return new InstanceExample(inst);
    }

    protected int classifyInstance(ClassificationStream.Node node, double[] attVals) {
        return node.children == null?node.classLabel:(node.splitAttIndex < this.numNominalsOption.getValue()?this.classifyInstance(node.children[(int)attVals[node.splitAttIndex]], attVals):this.classifyInstance(node.children[attVals[node.splitAttIndex] < node.splitAttValue?0:1], attVals));
    }

    protected void generateHeader() {
        FastVector attributes = new FastVector();
        FastVector nominalAttVals = new FastVector();

        int classLabels;
        for(classLabels = 0; classLabels < 5; ++classLabels) {
            nominalAttVals.addElement("value" + (classLabels + 1));
        }

        for(classLabels = 0; classLabels < 5; ++classLabels) {
            attributes.addElement(new Attribute("nominal" + (classLabels + 1), nominalAttVals));
        }

        for(classLabels = 0; classLabels < this.numNumericsOption.getValue(); ++classLabels) {
            attributes.addElement(new Attribute("numeric" + (classLabels + 1)));
        }

        FastVector var5 = new FastVector();

        for(int i = 0; i < this.numClassesOption.getValue(); ++i) {
            var5.addElement("class" + (i + 1));
        }

        attributes.addElement(new Attribute("class", var5));
        this.streamHeader = new InstancesHeader(new Instances(this.getCLICreationString(InstanceStream.class), attributes, 0));
        this.streamHeader.setClassIndex(this.streamHeader.numAttributes() - 1);
    }

    protected void generateRandomTree() {
        Random treeRand = new Random((long)this.treeRandomSeedOption.getValue());
        ArrayList nominalAttCandidates = new ArrayList(this.numNominalsOption.getValue());

        for(int minNumericVals = 0; minNumericVals < this.numNominalsOption.getValue(); ++minNumericVals) {
            nominalAttCandidates.add(Integer.valueOf(minNumericVals));
        }

        double[] var6 = new double[this.numNumericsOption.getValue()];
        double[] maxNumericVals = new double[this.numNumericsOption.getValue()];

        for(int i = 0; i < this.numNumericsOption.getValue(); ++i) {
            var6[i] = 0.0D;
            maxNumericVals[i] = 1.0D;
        }

        this.treeRoot = this.generateRandomTreeNode(0, nominalAttCandidates, var6, maxNumericVals, treeRand);
    }

    protected ClassificationStream.Node generateRandomTreeNode(int currentDepth, ArrayList<Integer> nominalAttCandidates, double[] minNumericVals, double[] maxNumericVals, Random treeRand) {
        ClassificationStream.Node node;
        if(currentDepth < this.maxTreeDepthOption.getValue() && (currentDepth < this.firstLeafLevelOption.getValue() || this.leafFractionOption.getValue() < 1.0D - treeRand.nextDouble())) {
            node = new ClassificationStream.Node();
            int chosenAtt =1;
            //int chosenAtt = treeRand.nextInt(nominalAttCandidates.size() + this.numNumericsOption.getValue());
            if(chosenAtt < nominalAttCandidates.size()) {
                node.splitAttIndex = ((Integer)nominalAttCandidates.get(chosenAtt)).intValue();
                node.children = new ClassificationStream.Node[this.numValsPerNominalOption.getValue()];
                ArrayList numericIndex = new ArrayList(nominalAttCandidates);
                numericIndex.remove(new Integer(node.splitAttIndex));
                numericIndex.trimToSize();

                for(int minVal = 0; minVal < node.children.length; ++minVal) {
                    node.children[minVal] = this.generateRandomTreeNode(currentDepth + 1, numericIndex, minNumericVals, maxNumericVals, treeRand);
                }
            } else {
                int var15 = chosenAtt - nominalAttCandidates.size();
                node.splitAttIndex = this.numNominalsOption.getValue() + var15;
                double var16 = minNumericVals[var15];
                double maxVal = maxNumericVals[var15];
                node.splitAttValue = (maxVal - var16) * treeRand.nextDouble() + var16;
                node.children = new ClassificationStream.Node[2];
                double[] newMaxVals = (double[])maxNumericVals.clone();
                newMaxVals[var15] = node.splitAttValue;
                node.children[0] = this.generateRandomTreeNode(currentDepth + 1, nominalAttCandidates, minNumericVals, newMaxVals, treeRand);
                double[] newMinVals = (double[])minNumericVals.clone();
                newMinVals[var15] = node.splitAttValue;
                node.children[1] = this.generateRandomTreeNode(currentDepth + 1, nominalAttCandidates, newMinVals, maxNumericVals, treeRand);
            }

            return node;
        } else {
            node = new ClassificationStream.Node();
            node.classLabel = treeRand.nextInt(this.numClassesOption.getValue());
            return node;
        }
    }

    public void getDescription(StringBuilder sb, int indent) {
    }

    public void setCepEvents(ConcurrentLinkedQueue<double[]> cepEvents) {
        this.cepEvents = cepEvents;
    }

    protected static class Node implements Serializable {
        private static final long serialVersionUID = 1L;
        public int classLabel;
        public int splitAttIndex;
        public double splitAttValue;
        public ClassificationStream.Node[] children;

        protected Node() {
        }
    }}