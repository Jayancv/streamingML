package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.samoa.evaluation.BasicClassificationPerformanceEvaluator;
import org.apache.samoa.evaluation.BasicRegressionPerformanceEvaluator;
import org.apache.samoa.evaluation.ClassificationPerformanceEvaluator;
import org.apache.samoa.evaluation.PerformanceEvaluator;
import org.apache.samoa.evaluation.RegressionPerformanceEvaluator;
import org.apache.samoa.learners.ClassificationLearner;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.RegressionLearner;
import org.apache.samoa.learners.classifiers.rules.VerticalAMRulesRegressor;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.tasks.Task;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.PrequentialSourceProcessor;
import org.apache.samoa.streams.generators.RandomTreeGenerator;
import org.apache.samoa.topology.ComponentFactory;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.Topology;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;
import com.github.javacliparser.FileOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;

/**
 * Created by wso2123 on 10/11/16.
 */


public class StreamingRegressionTask implements Task, Configurable {

    private static final long serialVersionUID = -8246537378371580550L;


    private static Logger logger = LoggerFactory.getLogger(StreamingRegression.class);

    public ClassOption learnerOption = new ClassOption("learner", 'l', "Learner.", Learner.class,
            VerticalAMRulesRegressor.class.getName());

    public ClassOption streamTrainOption = new ClassOption("trainStream", 's', "Stream to learn from.",
            InstanceStream.class,
            StreamingRegressionStream.class.getName());

    public ClassOption evaluatorOption = new ClassOption("evaluator", 'e',
            "Classification performance evaluation method.",
            PerformanceEvaluator.class, BasicRegressionPerformanceEvaluator.class.getName());

    public IntOption instanceLimitOption = new IntOption("instanceLimit", 'i',
            "Maximum number of instances to test/train on  (-1 = no limit).", 1000000, -1,
            Integer.MAX_VALUE);

    public IntOption timeLimitOption = new IntOption("timeLimit", 't',
            "Maximum number of seconds to test/train for (-1 = no limit).", -1, -1,
            Integer.MAX_VALUE);

    public IntOption sampleFrequencyOption = new IntOption("sampleFrequency", 'f',
            "How many instances between samples of the learning performance.", 100000,
            0, Integer.MAX_VALUE);

    public StringOption evaluationNameOption = new StringOption("evaluationName", 'n', "Identifier of the evaluation",
            "Prequential_"
                    + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));

    public FileOption dumpFileOption = new FileOption("dumpFile", 'd', "File to append intermediate csv results to",
            null, "csv", true);

    // Default=0: no delay/waiting
    public IntOption sourceDelayOption = new IntOption("sourceDelay", 'w',
            "How many microseconds between injections of two instances.", 0, 0, Integer.MAX_VALUE);
    // Batch size to delay the incoming stream: delay of x milliseconds after each
    // batch
    public IntOption batchDelayOption = new IntOption("delayBatchSize", 'b',
            "The delay batch size: delay of x milliseconds after each batch ", 1, 1, Integer.MAX_VALUE);

   // protected PrequentialSourceProcessor preqSource;

    // private PrequentialSourceTopologyStarter preqStarter;

    // private EntranceProcessingItem sourcePi;

    protected StreamingRegressionEntranceProcessor preqSource;
    private InstanceStream streamTrain;
    protected Stream sourcePiOutputStream;
    private Learner reggressionCal;
    private StreamingRegressionEvaluationProcessor evaluator;

    // private ProcessingItem evaluatorPi;

    // private Stream evaluatorPiInputStream;

    protected Topology prequentialTopology;
    protected TopologyBuilder builder;

    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<Vector> predictions;


    public void getDescription(StringBuilder sb, int indent) {
        sb.append("Prequential evaluation");
    }

    @Override
    public void init() {
        // TODO remove the if statement
        // theoretically, dynamic binding will work here!
        // test later!
        // for now, the if statement is used by Storm
        streamTrain = this.streamTrainOption.getValue();

        if (streamTrain instanceof StreamingRegressionStream) {
            logger.info("Stream is a StreamingRegressionStream");
            StreamingRegressionStream myStream = (StreamingRegressionStream) streamTrain;
            myStream.setCepEvent(this.cepEvents);
        } else {
            logger.info("Check stream: Stream is not a StreamingRegressionStream");
        }

        if (builder == null) {
            builder = new TopologyBuilder();
            logger.debug("Successfully instantiating TopologyBuilder");

            builder.initTopology(evaluationNameOption.getValue());
            logger.debug("Successfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());
        }

        // instantiate PrequentialSourceProcessor and its output stream
        // (sourcePiOutputStream)
        preqSource = new StreamingRegressionEntranceProcessor();
        preqSource.setStreamSource(streamTrain);
        builder.addEntranceProcessor(preqSource);
        preqSource.setMaxNumInstances(instanceLimitOption.getValue());
        preqSource.setSourceDelay(sourceDelayOption.getValue());
        preqSource.setDelayBatchSize(batchDelayOption.getValue());

        logger.debug("Successfully instantiating PrequentialSourceProcessor");

        // preqStarter = new PrequentialSourceTopologyStarter(preqSource,
        // instanceLimitOption.getValue());
        // sourcePi = builder.createEntrancePi(preqSource, preqStarter);
        // sourcePiOutputStream = builder.createStream(sourcePi);

        sourcePiOutputStream = builder.createStream(preqSource);
        // preqStarter.setInputStream(sourcePiOutputStream);

        // instantiate classifier and connect it to sourcePiOutputStream
        reggressionCal = this.learnerOption.getValue();
        reggressionCal.init(builder, preqSource.getDataset(), 1);
        builder.connectInputShuffleStream(sourcePiOutputStream, reggressionCal.getInputProcessor());
        logger.debug("Successfully instantiating Regression Calculator");

        PerformanceEvaluator evaluatorOptionValue = this.evaluatorOption.getValue();
        if (!StreamingRegressionTask.isLearnerAndEvaluatorCompatible(reggressionCal, evaluatorOptionValue)) {
            evaluatorOptionValue = getDefaultPerformanceEvaluatorForLearner(reggressionCal);
        }
        evaluator = new StreamingRegressionEvaluationProcessor.Builder(evaluatorOptionValue)
                .samplingFrequency(sampleFrequencyOption.getValue()).dumpFile(dumpFileOption.getFile()).build();

        // evaluatorPi = builder.createPi(evaluator);
        // evaluatorPi.connectInputShuffleStream(evaluatorPiInputStream);
        evaluator.setPredictions((ArrayList<Integer>) reggressionCal);
        builder.addProcessor(evaluator);
        for (Stream evaluatorPiInputStream : reggressionCal.getResultStreams()) {
            builder.connectInputShuffleStream(evaluatorPiInputStream, evaluator);
        }

        logger.debug("Successfully instantiating EvaluatorProcessor");

        prequentialTopology = builder.build();
        logger.debug("Successfully building the topology");
    }

    @Override
    public void setFactory(ComponentFactory factory) {
        // TODO unify this code with init()
        // for now, it's used by S4 App
        // dynamic binding theoretically will solve this problem
        builder = new TopologyBuilder(factory);
        logger.debug("Successfully instantiating TopologyBuilder");

        builder.initTopology(evaluationNameOption.getValue());
        logger.debug("Successfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());

    }

    public Topology getTopology() {
        return prequentialTopology;
    }

    //
    // @Override
    // public TopologyStarter getTopologyStarter() {
    // return this.preqStarter;
    // }

    protected static boolean isLearnerAndEvaluatorCompatible(Learner learner, PerformanceEvaluator evaluator) {
        return (learner instanceof RegressionLearner && evaluator instanceof RegressionPerformanceEvaluator) ||
                (learner instanceof ClassificationLearner && evaluator instanceof ClassificationPerformanceEvaluator);
    }

    protected static PerformanceEvaluator getDefaultPerformanceEvaluatorForLearner(Learner learner) {
        if (learner instanceof RegressionLearner) {
            return new BasicRegressionPerformanceEvaluator();
        }
        // Default to BasicClassificationPerformanceEvaluator for all other cases
        return new BasicClassificationPerformanceEvaluator();
    }

    public void setCepEvents(ConcurrentLinkedQueue<double[]> cepEvents) {
        this.cepEvents = cepEvents;
    }

    public void setSamoaClassifiers(ConcurrentLinkedQueue<Vector> classifiers) {
        this.predictions = classifiers;

    }

}
