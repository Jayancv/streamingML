package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression;

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
 * Created by wso2123 on 10/11/16.
 */
public class StreamingRegressionTaskBuilder {

    private static final String SUPPRESS_STATUS_OUT_MSG = "Suppress the task status output. Normally it is sent to stderr.";
    private static final String SUPPRESS_RESULT_OUT_MSG = "Suppress the task result output. Normally it is sent to stdout.";
    private static final String STATUS_UPDATE_FREQ_MSG = "Wait time in milliseconds between status updates.";

    private static final Logger logger = LoggerFactory.getLogger(StreamingRegressionTaskBuilder.class);

    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<Vector> samoaPredictions;

    public int maxInstances = 1000000;
    public int batchSize = 1000;
    public int numAttr = 2;
    public int parallel = 1;
    public int bagging = 1;

    public StreamingRegressionTaskBuilder(int maxInstance, int batchSize, int numAtts, ConcurrentLinkedQueue<double[]> cepEvents, ConcurrentLinkedQueue<Vector> data, int par, int bag) {
        logger.info("Streaming  StreamingRegression TaskBuilder");
        this.cepEvents = cepEvents;
        this.maxInstances = maxInstance;
        this.batchSize = batchSize;
        this.numAttr = numAtts;
        this.samoaPredictions = data;
        this.parallel = par;
        this.bagging = bag;

    }

    public void initTask(int maxInstance, int batchSize, int numAttributes, int paralle, int bagging) {


        String query = "";
        if (bagging != 0) {
            //---------Bagging
            query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression.StreamingRegressionTask -f " + batchSize + " -i " + maxInstances + " -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression.StreamingRegressionStream -A " + numAttributes + ") -l (classifiers.ensemble.Bagging -s " + bagging + " -l (org.apache.samoa.learners.classifiers.rules.VerticalAMRulesRegressor -p " + paralle + "))";
        } else {
            query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression.StreamingRegressionTask -f " + batchSize + " -i " + maxInstances + " -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression.StreamingRegressionStream -A " + numAttributes + " ) -l   (org.apache.samoa.learners.classifiers.rules.HorizontalAMRulesRegressor -r 9 -p "+paralle+")";

            //query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression.StreamingRegressionTask -f " + batchSize + " -i " + maxInstances + " -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression.StreamingRegressionStream -A " + numAttributes + " ) -l (org.apache.samoa.learners.classifiers.rules.VerticalAMRulesRegressor -p " + paralle + ")";
        }



        logger.info("QUERY: " + query);
        String args[] = {query};
        this.initRegressionTask(args);
    }

    private void initRegressionTask(String[] args) {

        logger.info("Initializing Samoa StreamingRegression Topology");

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
            task = ClassOption.cliStringToObject(cliString.toString(), Task.class, extraOptions);
            logger.info("Successfully instantiating {}", task.getClass().getCanonicalName());
        } catch (Exception e) {
            logger.error("Fail to initialize the task", e);
            logger.info("Fail to initialize the task" + e);
            return;
        }

        logger.info("A");
        if (task instanceof StreamingRegressionTask) {
            logger.info("Task is a Instance of StreamingRegressionTask");
            StreamingRegressionTask t = (StreamingRegressionTask) task;
            t.setCepEvents(this.cepEvents);
            t.setSamoaData(this.samoaPredictions);

        } else {
            logger.info("Check Task: Not a StreamingRegressionTask");
        }
        logger.info("Successfully Convert the Task into StreamingRegressionTask");
        task.setFactory(new SimpleComponentFactory());
        logger.info("Successfully Initialized Component Factory");
        task.init();
        logger.info("Successfully Initiated the StreamingRegressionTask");
        SimpleEngine.submitTopology(task.getTopology());
        logger.info("Samoa Simple Engine Started");

    }
}
