package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.Option;
import org.apache.samoa.moa.classifiers.AbstractClassifier;
import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.impl.SimpleComponentFactory;
import org.apache.samoa.topology.impl.SimpleEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by wso2123 on 8/30/16.
 */
public class ClassificationTaskBuilder {


    // It seems that the 3 extra options are not used.
    // Probably should remove them
    private static final String SUPPRESS_STATUS_OUT_MSG = "Suppress the task status output. Normally it is sent to stderr.";
    private static final String SUPPRESS_RESULT_OUT_MSG = "Suppress the task result output. Normally it is sent to stdout.";
    private static final String STATUS_UPDATE_FREQ_MSG = "Wait time in milliseconds between status updates.";
    private static final Logger logger = LoggerFactory.getLogger(ClassificationTaskBuilder.class);


    public ConcurrentLinkedQueue<double[]> cepEvents;
  //  public ConcurrentLinkedQueue<AbstractClassifier> samoaClassifiers;
    public int numClasses = 2;
    public int maxInstances = 100000;

    public ClassificationTaskBuilder(int numClasses, ConcurrentLinkedQueue<double[]> cepEvents, ConcurrentLinkedQueue<AbstractClassifier> samoaClassifiers, int maxInstance) {

        logger.info("Streaming  Classification TaskBuilder");
        this.numClasses = numClasses;
        this.cepEvents = cepEvents;
      ////  this.samoaClassifiers = samoaClassifiers;
        this.maxInstances = maxInstance;

    }

    public void initTask(int numAttributes, int numClasses, int maxInstances) {
        String query = "";
        //query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification.ClassTask -f 100000 -i 1000000 -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification.ClassificationStream ) -l (org.apache.samoa.learners.classifiers.SingleClassifier -l (org.apache.samoa.learners.classifiers.SimpleClassifierAdapter -l (org.apache.samoa.moa.classifiers.functions.MajorityClass)))";
      //  query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification.ClassTask -f 100000 -i 1000000 -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification.ClassificationStream ) -l (org.apache.samoa.learners.classifiers.ensemble.Bagging -l (org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree))";
         query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification.ClassTask -f 100000 -i 1000000 -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification.ClassificationStream ) -l (org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree)";
       // query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification.ClassTask -f 100000 -i 1000000 -s (org.apache.samoa.streams.generators.RandomTreeGenerator ) -l (org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree)";

        logger.info("QUERY: " + query);
        String args[] = {query};
        this.initClassificationTask(args);
    }

    private void initClassificationTask(String[] args) {
        System.out.println("done toooo");
        logger.info("Initializing Samoa Classification Topology");

        FlagOption suppressStatusOutOpt = new FlagOption("suppressStatusOut", 'S', SUPPRESS_STATUS_OUT_MSG);
        FlagOption suppressResultOutOpt = new FlagOption("suppressResultOut", 'R', SUPPRESS_RESULT_OUT_MSG);
        IntOption statusUpdateFreqOpt = new IntOption("statusUpdateFrequency", 'F', STATUS_UPDATE_FREQ_MSG, 1000, 0, Integer.MAX_VALUE);

        Option[] extraOptions = new Option[]{suppressStatusOutOpt, suppressResultOutOpt, statusUpdateFreqOpt};

        StringBuilder cliString = new StringBuilder();
        for (String arg : args) {
            logger.info(arg);
            cliString.append(" ").append(arg);
        }
        logger.debug("Command line string = {}", cliString.toString());
        logger.info("Command line string = " + cliString.toString());


        Task task;
        try {
            logger.info(String.valueOf(cliString));
            task = ClassOption.cliStringToObject(cliString.toString(), Task.class, extraOptions);
            logger.info("Successfully instantiating {}", task.getClass().getCanonicalName());
        } catch (Exception e) {
            logger.error("Fail to initialize the task", e);
            logger.info("Fail to initialize the task" + e);
            return;
        }

        //task = new StreamingClusteringTask();
        logger.info("A");
        if (task instanceof ClassTask) {
            logger.info("Task is a Instance of StreamingClassificationTask");
            ClassTask t = (ClassTask) task;
            t.setCepEvents(this.cepEvents);
          //  t.setSamoaClassifiers(this.samoaClassifiers);
            t.setNumClasses(this.numClasses);

        } else {

            logger.info("Check Task: Not a StreamingClassificationTask");
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



