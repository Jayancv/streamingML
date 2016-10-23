package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification;

/**
 * Created by wso2123 on 9/6/16.
 */

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.samoa.evaluation.*;
import org.apache.samoa.learners.ClassificationLearner;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.RegressionLearner;
import org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.tasks.Task;
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


public class StreamingClassificationTask implements Task, Configurable {

    private static final long serialVersionUID = -8246537378371580550L;

    private static Logger logger = LoggerFactory.getLogger(StreamingClassificationTask.class);

    public ClassOption learnerOption = new ClassOption("learner", 'l', "Classifier to train.", Learner.class,
            VerticalHoeffdingTree.class.getName());

    public ClassOption streamTrainOption = new ClassOption("trainStream", 's', "Stream to learn from.",
            InstanceStream.class, StreamingClassificationStream.class.getName());

    public ClassOption evaluatorOption = new ClassOption("evaluator", 'e',
            "StreamingClassification performance evaluation method.",
            PerformanceEvaluator.class, StreamingClassificationPerformanceEvaluator.class.getName());

    public IntOption instanceLimitOption = new IntOption("instanceLimit", 'i',
            "Maximum number of instances to test/train on  (-1 = no limit).", 1000000, -1,
            Integer.MAX_VALUE);

    public IntOption timeLimitOption = new IntOption("timeLimit", 't',
            "Maximum number of seconds to test/train for (-1 = no limit).", -1, -1,
            Integer.MAX_VALUE);

    public IntOption sampleFrequencyOption = new IntOption("sampleFrequency", 'f',
            "How many instances between samples of the learning performance.", 1000,
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

    protected StreamingClassificationEntranceProcessor preqSource;
    private InstanceStream streamTrain;
    protected Stream sourcePiOutputStream;
    private Learner classifier;
    private StreamingClassificationEvaluationProcessor evaluator;

    protected Topology prequentialTopology;
    protected TopologyBuilder builder;

    public ConcurrentLinkedQueue<double[]> cepEvents;
    public ConcurrentLinkedQueue<Vector> classifiers;
    public int numClasses = 2;


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

        if (streamTrain instanceof StreamingClassificationStream) {
            logger.info("Stream is a StreamingClassificationStream");
            StreamingClassificationStream myStream = (StreamingClassificationStream) streamTrain;
            myStream.setCepEvents(this.cepEvents);
        } else {
            logger.info("Check Stream: Stream is not a StreamingClusteringStream");
        }


        if (builder == null) {
            builder = new TopologyBuilder();
            logger.debug("Successfully instantiating TopologyBuilder");

            builder.initTopology(evaluationNameOption.getValue());
            logger.debug("Successfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());
        }


        preqSource = new StreamingClassificationEntranceProcessor();

        preqSource.setStreamSource(streamTrain);
        builder.addEntranceProcessor(preqSource);
        preqSource.setMaxNumInstances(instanceLimitOption.getValue());
        preqSource.setSourceDelay(sourceDelayOption.getValue());
        preqSource.setDelayBatchSize(batchDelayOption.getValue());

        logger.debug("Successfully instantiating PrequentialSourceProcessor");
        logger.info("Successfully instantiating PrequentialSourceProcessor");

        sourcePiOutputStream = builder.createStream(preqSource);

        classifier = this.learnerOption.getValue();
        classifier.init(builder, preqSource.getDataset(), 1);
        builder.connectInputShuffleStream(sourcePiOutputStream, classifier.getInputProcessor());
        logger.debug("Successfully instantiating Classifier");
        logger.info("Successfully instantiating Classifier");

        PerformanceEvaluator evaluatorOptionValue = this.evaluatorOption.getValue();
        if (!StreamingClassificationTask.isLearnerAndEvaluatorCompatible(classifier, evaluatorOptionValue)) {
            evaluatorOptionValue = getDefaultPerformanceEvaluatorForLearner(classifier);
        }
        evaluator = new StreamingClassificationEvaluationProcessor.Builder(evaluatorOptionValue)
                .samplingFrequency(sampleFrequencyOption.getValue()).dumpFile(dumpFileOption.getFile()).build();

        evaluator.setSamoaClassifiers(classifiers);
        builder.addProcessor(evaluator);
        for (Stream evaluatorPiInputStream : classifier.getResultStreams()) {
            builder.connectInputShuffleStream(evaluatorPiInputStream, evaluator);
        }

        logger.debug("Successfully instantiating EvaluatorProcessor");
        logger.info("Successfully instantiating EvaluatorProcessor");

        prequentialTopology = builder.build();
        logger.debug("Successfully building the topology");
        logger.info("Successfully building the topology");
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
        this.classifiers = classifiers;

    }

    public void setNumClasses(int numClasses) {
        this.numClasses = numClasses;
    }
}