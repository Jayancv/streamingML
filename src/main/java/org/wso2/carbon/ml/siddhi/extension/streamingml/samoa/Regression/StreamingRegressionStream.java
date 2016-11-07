package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression;

import com.github.javacliparser.IntOption;
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
 * Created by wso2123 on 10/11/16.
 */
public class StreamingRegressionStream extends ClusteringStream {

    private static final Logger logger = LoggerFactory.getLogger(StreamingRegressionStream.class);
    private ConcurrentLinkedQueue<double[]> cepEvent;
    protected InstancesHeader streamHeader;
    private int numGeneratedInstances;
    private int nextEventCounter;
    LinkedList<DataPoint> points = new LinkedList<DataPoint>();
    double[] values; //Cep Event
    private int numAttributes = 2;

    public IntOption numAttOption = new IntOption("numAttributes", 'A', "The number of classes in the model.", 2, 1, Integer.MAX_VALUE);


    @Override
    protected void prepareForUseImpl(TaskMonitor taskMonitor, ObjectRepository objectRepository) {

        taskMonitor.setCurrentActivity("Preparing random RBF...", -1.0);
        this.numAttributes = numAttOption.getValue();

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

        for (int i = 0; i < numAttributes; i++) {
            attributes.add(new Attribute("numeric" + (i + 1)));
        }
        streamHeader = new InstancesHeader(new Instances(getCLICreationString(InstanceStream.class), attributes, 0));
        streamHeader.setClassIndex(streamHeader.numAttributes() - 1);
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
            while (cepEvent == null) ;
            while (cepEvent.isEmpty()) ;

            double[] values = cepEvent.poll();

            System.arraycopy(values, 0, values_new, 0, values.length-1);
            Instance inst = new DenseInstance(1.0, values_new);
            inst.setDataset(getHeader());

            // Set the relevant class value to the data set
            inst.setClassValue(values[values.length - 1]);
           // inst.setClassValue(values_new[values.length - 1]);
            return new InstanceExample(inst);

        } else {
           // System.out.println(numGeneratedInstances);
            numGeneratedInstances++;

            double[] values_new = new double[numAttributes]; // +1
            while (cepEvent.isEmpty()) ;
            double[] values = cepEvent.poll();

            System.arraycopy(values, 0, values_new, 0, values.length-1);
            Instance inst = new DenseInstance(1.0, values_new);
            inst.setDataset(getHeader());


            inst.setClassValue(values[values.length - 1]);
            //System.out.println(inst.classValue());
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

    public void setCepEvent(ConcurrentLinkedQueue<double[]> cepEvent) {
        this.cepEvent = cepEvent;
    }

}
