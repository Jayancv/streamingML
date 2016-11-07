package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.Option;
import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.impl.SimpleComponentFactory;
import org.apache.samoa.topology.impl.SimpleEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by wso2123 on 8/30/16.
 */
public class StreamingClassificationTaskBuilder {


    // It seems that the 3 extra options are not used.
    // Probably should remove them
    private static final String SUPPRESS_STATUS_OUT_MSG = "Suppress the task status output. Normally it is sent to stderr.";
    private static final String SUPPRESS_RESULT_OUT_MSG = "Suppress the task result output. Normally it is sent to stdout.";
    private static final String STATUS_UPDATE_FREQ_MSG = "Wait time in milliseconds between status updates.";
    private static final Logger logger = LoggerFactory.getLogger(StreamingClassificationTaskBuilder.class);


    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<Vector> classifiers;

    public int maxInstances = 100000;
    public int batchSize = 1000;
    public int numClasses = 4;
    public int numAttr = 2;
    public int numNominalAtt = 0;
    public int parallel = 1;
    public int bagging = 1;


    public StreamingClassificationTaskBuilder(int maxInstance, int batchSize, int numClasses, int numAtts, int numNominals, ConcurrentLinkedQueue<double[]> cepEvents, ConcurrentLinkedQueue<Vector> classifiers, int par, int bag) {

        logger.info("Streaming  StreamingClassification TaskBuilder");
        this.numClasses = numClasses;
        this.cepEvents = cepEvents;
        this.maxInstances = maxInstance;
        this.numAttr = numAtts;
        this.numNominalAtt = numNominals;
        this.batchSize = batchSize;
        this.classifiers = classifiers;
        this.parallel = par;
        this.bagging = bag;
    }

    public void initTask(int maxInstances, int batchSize, int numClasses, int numAttributes, int numNominals, String str, int parallel, int bagging) {
        String query = "";

        if (bagging != 0) {
            //---------Bagging
            query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationTask -f " + batchSize + " -i " + maxInstances + " -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationStream -K " + numClasses + " -A " + numAttributes + " -N " + numNominals + " -Z " + str + " ) -l (classifiers.ensemble.Bagging -s " + bagging + " -l (classifiers.trees.VerticalHoeffdingTree -p " + parallel + "))";
        } else {
            query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationTask -f " + batchSize + " -i " + maxInstances + " -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationStream -K " + numClasses + " -A " + numAttributes + " -N " + numNominals + " -Z " + str + " ) -l (org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree -p " + parallel + ")";

        }
        //--------Adaptive Bagging
        // query ="org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationTask -f " + batchSize + " -i " + maxInstances + " -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationStream -K " + numClasses + " -A "+numAttributes+" -N "+numNominals+" -Z "+str+" ) -l (classifiers.ensemble.Bagging -s 10 -l (classifiers.ensemble.AdaptiveBagging -s 10 -l (classifiers.trees.VerticalHoeffdingTree -p 10)))";
        //--------Boosting
        // query ="org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationTask -f " + batchSize + " -i " + maxInstances + " -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationStream -K " + numClasses + " -A "+numAttributes+" -N "+numNominals+" -Z "+str+" ) -l (classifiers.ensemble.Boosting -s 10 -l (classifiers.trees.VerticalHoeffdingTree -p 10))";

        //query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationTask -f 1000 -i 1000000 -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationStream -K " + numAttributes + " ) -l (org.apache.samoa.learners.classifiers.ensemble.Bagging -l (org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree))";
        /////////////////////////////////////////////   query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationTask -f 1000 -i 1000000 -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationStream  -K " + numAttributes + " ) -l (org.apache.samoa.learners.classifiers.SingleClassifier -l (org.apache.samoa.learners.classifiers.SimpleClassifierAdapter -l (org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree)))";

        //--------RandomTreeGeneration
        // query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification.StreamingClassificationTask -f 1000 -i 1000000 -s (org.apache.samoa.streams.generators.RandomTreeGenerator ) -l (org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree)";
        //2,2,2,2,2,2,2,2,2,2,2,2,2,2,3,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,3

        logger.info("QUERY: " + query);
        String args[] = {query};
        this.initClassificationTask(args);
    }

    private void initClassificationTask(String[] args) {

        logger.info("Initializing Samoa StreamingClassification Topology");

        /// No idea about about these variables
        FlagOption suppressStatusOutOpt = new FlagOption("suppressStatusOut", 'S', SUPPRESS_STATUS_OUT_MSG);
        FlagOption suppressResultOutOpt = new FlagOption("suppressResultOut", 'R', SUPPRESS_RESULT_OUT_MSG);
        IntOption statusUpdateFreqOpt = new IntOption("statusUpdateFrequency", 'F', STATUS_UPDATE_FREQ_MSG, 1000, 0, Integer.MAX_VALUE);
        Option[] extraOptions = new Option[]{suppressStatusOutOpt, suppressResultOutOpt, statusUpdateFreqOpt};
        ///

        StringBuilder cliString = new StringBuilder();
        for (String arg : args) {
            logger.info(arg);
            cliString.append(" ").append(arg);
        }
        logger.debug("Command line string = {}", cliString.toString());

        Task task;
        try {
            logger.info(String.valueOf(cliString));
            task = ClassOption.cliStringToObject(cliString.toString(), Task.class, extraOptions);       // Convert that query to a Object of a Task
            logger.info("Successfully instantiating {}", task.getClass().getCanonicalName());
        } catch (Exception e) {
            logger.error("Fail to initialize the task", e);
            return;
        }

        if (task instanceof StreamingClassificationTask) {
            logger.info("Task is a Instance of StreamingClassificationTask");
            StreamingClassificationTask t = (StreamingClassificationTask) task;  // Cast that created task to streamingclassificationTask
            t.setCepEvents(this.cepEvents);                                      //link task events to cepEvents
            t.setSamoaClassifiers(this.classifiers);
            t.setNumClasses(this.numClasses);

        } else {
            logger.info("Check Task:It is not a StreamingClassificationTask");
        }

        logger.info("Successfully Convert the Task into StreamingClassificationTask");
        task.setFactory(new SimpleComponentFactory());
        logger.info("Successfully Initialized Component Factory");
        task.init();
        logger.info("Successfully Initiated the StreamingClassificationTask");
        SimpleEngine.submitTopology(task.getTopology());
        logger.info("Samoa Simple Engine Started");
    }
}



