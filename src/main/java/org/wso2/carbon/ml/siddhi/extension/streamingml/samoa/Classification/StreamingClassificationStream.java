package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification;

import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;
import org.apache.samoa.instances.*;
import org.apache.samoa.moa.core.DataPoint;
import org.apache.samoa.moa.core.Example;
import org.apache.samoa.moa.core.InstanceExample;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.clustering.ClusteringStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by wso2123 on 9/26/16.
 */
public class StreamingClassificationStream extends ClusteringStream {


    public ConcurrentLinkedQueue<double[]> cepEvents;
    private static final Logger logger = LoggerFactory.getLogger(StreamingClassificationStream.class);
    protected InstancesHeader streamHeader;
    private int numGeneratedInstances;
    private int nextEventCounter;
    LinkedList<DataPoint> points = new LinkedList<DataPoint>();
    double[] values; //Cep Event
    private int numAttributes = 2;
    private int numNorminals = 0;
    private int numClasses = 2;

    ArrayList<Integer> valsForNominals = new ArrayList<Integer>();

    public IntOption numClassesOption = new IntOption("numClasses", 'K', "The number of classes in the model.", 2, 2, Integer.MAX_VALUE);
    public IntOption numAttOption = new IntOption("numAttributes", 'A', "The number of classes in the model.", 2, 1, Integer.MAX_VALUE);
    public IntOption numNominalsOption = new IntOption("numNominals", 'N', "The number of nominal attributes to generate.", 0, 0, 2147483647);
    public IntOption numNumericsOption = new IntOption("numNumerics", 'u', "The number of numeric attributes to generate.", 1, 0, 2147483647);

    public StringOption numValsPerNominalOption = new StringOption("numValsPerNominalOption", 'Z', "The number of values per nominal attributes", "null");

    @Override
    protected void prepareForUseImpl(TaskMonitor taskMonitor, ObjectRepository objectRepository) {
        taskMonitor.setCurrentActivity("Preparing random RBF...", -1.0);

        this.numClasses = numClassesOption.getValue();
        this.numAttributes = numAttOption.getValue();
        this.numNorminals = numNominalsOption.getValue();
        if (numNorminals != 0) {
            String[] valsForNominal = numValsPerNominalOption.getValue().split(",");
            for (String i : valsForNominal) {
                valsForNominals.add(Integer.parseInt(i));
            }
        }

        logger.info("Number of Attributes in the Stream : " + this.numAttributes);
        generateHeader();
        restart();
        values = new double[numAttributes];

        for (int i = 0; i < numAttributes; i++) {
            values[i] = 0;
        }
    }

    private void generateHeader() {
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
        ArrayList<String> nominalAttValls = new ArrayList<>();
        for (int i = 0; i < numAttributes - 1 - numNominalsOption.getValue(); i++) {
            attributes.add(new Attribute("numeric" + (i + 1)));
        }


        for (int i = 0; i < this.numNominalsOption.getValue(); ++i) {
            attributes.add(new Attribute("nominal" + (i + 1), getvals(i)));
        }

        ArrayList<String> classLabels = new ArrayList<String>();
        for (int i = 0; i < this.numClassesOption.getValue(); i++) {
            classLabels.add("class" + (i + 1));
        }

        attributes.add(new Attribute("class", classLabels));
        streamHeader = new InstancesHeader(new Instances(getCLICreationString(InstanceStream.class), attributes, 0));
        streamHeader.setClassIndex(streamHeader.numAttributes() - 1);
    }

    private ArrayList<String> getvals(int count) {
        ArrayList<String> nominalAttValls = new ArrayList<>();

        for (int i = 0; i < valsForNominals.get(count); ++i) {
            nominalAttValls.add("value" + (i + 1));
        }


        return nominalAttValls;
    }

    @Override
    public InstancesHeader getHeader() {
        return streamHeader;
    }

    @Override
    public long estimatedRemainingInstances() {
        return -1L;
    }

    @Override
    public boolean hasMoreInstances() {
        return true;
    }

    @Override
    public Example<Instance> nextInstance() {
        if (numGeneratedInstances == 0) {
            logger.info("Sending First Samoa Instance.....");
            numGeneratedInstances++;
            double[] values_new = new double[numAttributes]; // +1
            while (cepEvents == null) ;
            while (cepEvents.isEmpty()) ;

            double[] values = cepEvents.poll();

            System.arraycopy(values, 0, values_new, 0, values.length - 1);
            Instance inst = new DenseInstance(1.0, values_new);
            inst.setDataset(getHeader());
            //inst.numClasses();
            inst.setClassValue(values[values.length - 1]);
            return new InstanceExample(inst);

        } else {
            numGeneratedInstances++;
            double[] values_new = new double[numAttributes]; // +1

            while (cepEvents.isEmpty()) ;
            double[] values = cepEvents.poll();

            System.arraycopy(values, 0, values_new, 0, values.length - 1);
            Instance inst = new DenseInstance(1.0, values_new);
            inst.setDataset(getHeader());

            inst.setClassValue(values[values.length - 1]);

            return new InstanceExample(inst);
        }
    }

    @Override
    public boolean isRestartable() {
        return true;
    }

    @Override
    public void restart() {
        numGeneratedInstances = 0;

    }

    @Override
    public void getDescription(StringBuilder stringBuilder, int i) {

    }

    public void setCepEvents(ConcurrentLinkedQueue<double[]> cepEvents) {
        this.cepEvents = cepEvents;
    }


}
