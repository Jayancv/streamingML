package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression;

import com.github.javacliparser.IntOption;
import org.apache.samoa.instances.Attribute;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.moa.core.DataPoint;
import org.apache.samoa.moa.core.Example;
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

    private static final Logger logger= LoggerFactory.getLogger(StreamingRegressionStream.class);
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

        for (int i = 0; i < numAttributes-1; i++) {
            attributes.add(new Attribute("numeric" + (i + 1)));
        }


        ArrayList<String> classLabels = new ArrayList<String>();

        attributes.add(new Attribute("class", classLabels));
        streamHeader = new InstancesHeader(new Instances(getCLICreationString(InstanceStream.class), attributes, 0));
        streamHeader.setClassIndex(streamHeader.numAttributes() - 1);
    }

    @Override
    public InstancesHeader getHeader() {
        return null;
    }

    @Override
    public long estimatedRemainingInstances() {
        return 0;
    }

    @Override
    public boolean hasMoreInstances() {
        return false;
    }

    @Override
    public Example<Instance> nextInstance() {
        return null;
    }

    @Override
    public boolean isRestartable() {
        return false;
    }

    @Override
    public void restart() {

    }

    @Override
    public void getDescription(StringBuilder stringBuilder, int i) {

    }
    public void setCepEvent(ConcurrentLinkedQueue<double[]> cepEvent) {
        this.cepEvent = cepEvent;
    }

}
