package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

/**
 * Created by wso2123 on 9/6/16.
 */
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.samoa.evaluation.BasicClassificationPerformanceEvaluator;
import org.apache.samoa.evaluation.BasicRegressionPerformanceEvaluator;
import org.apache.samoa.evaluation.ClassificationPerformanceEvaluator;
import org.apache.samoa.evaluation.EvaluatorProcessor;
import org.apache.samoa.evaluation.PerformanceEvaluator;
import org.apache.samoa.evaluation.RegressionPerformanceEvaluator;
import org.apache.samoa.learners.ClassificationLearner;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.RegressionLearner;
import org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.generators.RandomTreeGenerator;
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

/**
 * Prequential Evaluation task is a scheme in evaluating performance of online classifiers which uses each instance for
 * testing online classifiers model and then it further uses the same instance for training the model(Test-then-train)
 *
 * @author Arinto Murdopo
 *
 */
public class ClassTask implements Task, Configurable {

    private static final long serialVersionUID = -8246537378371580550L;

    private static Logger logger = LoggerFactory.getLogger(ClassTask.class);

    public ClassOption learnerOption = new ClassOption("learner", 'l', "Classifier to train.", Learner.class,
            VerticalHoeffdingTree.class.getName());

    public ClassOption streamTrainOption = new ClassOption("trainStream", 's', "Stream to learn from.",
            InstanceStream.class,
            RandomTreeGenerator.class.getName());

    public ClassOption evaluatorOption = new ClassOption("evaluator", 'e',
            "Classification performance evaluation method.",
            PerformanceEvaluator.class, BasicClassificationPerformanceEvaluator.class.getName());

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

    protected ClassificationEntranceProcessor preqSource;
    private InstanceStream streamTrain;
    protected Stream sourcePiOutputStream;
    private Learner classifier;
    private EvaluatorProcessor evaluator;

    // private ProcessingItem evaluatorPi;

    // private Stream evaluatorPiInputStream;

    protected Topology prequentialTopology;
    protected TopologyBuilder builder;

    public ConcurrentLinkedQueue<double[]> cepEvents;
    public int numClasses=2;


    public void getDescription(StringBuilder sb, int indent) {
        sb.append("Prequential evaluation");
    }

    @Override
    public void init() {
        // TODO remove the if statement
        // theoretically, dynamic binding will work here!
        // test later!
        // for now, the if statement is used by Storm

        if (builder == null) {
            builder = new TopologyBuilder();
            logger.debug("Successfully instantiating TopologyBuilder");

            builder.initTopology(evaluationNameOption.getValue());
            logger.debug("Successfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());
        }

        // instantiate PrequentialSourceProcessor and its output stream
        // (sourcePiOutputStream)
        preqSource = new ClassificationEntranceProcessor();

        streamTrain=this.streamTrainOption.getValue();

        if(streamTrain instanceof ClassificationStream){
            logger.info("Stream is a StreamingClusteringStream");
            ClassificationStream myStream = (ClassificationStream)streamTrain;
            myStream.setCepEvents(this.cepEvents);
        }else{
            logger.info("Check Stream: Stream is not a StreamingClusteringStream");
        }


       // preqSource.setStreamSource((InstanceStream) this.streamTrainOption.getValue());
        preqSource.setStreamSource(streamTrain);
        preqSource.setMaxNumInstances(instanceLimitOption.getValue());
        preqSource.setSourceDelay(sourceDelayOption.getValue());
        preqSource.setDelayBatchSize(batchDelayOption.getValue());
        builder.addEntranceProcessor(preqSource);
        logger.debug("Successfully instantiating PrequentialSourceProcessor");


        sourcePiOutputStream = builder.createStream(preqSource);
        // preqStarter.setInputStream(sourcePiOutputStream);

        // instantiate classifier and connect it to sourcePiOutputStream
        classifier = this.learnerOption.getValue();
        classifier.init(builder, preqSource.getDataset(), 1);
        builder.connectInputShuffleStream(sourcePiOutputStream, classifier.getInputProcessor());
        logger.debug("Successfully instantiating Classifier");

//        PerformanceEvaluator evaluatorOptionValue = this.evaluatorOption.getValue();
//        if (!ClassTask.isLearnerAndEvaluatorCompatible(classifier, evaluatorOptionValue)) {
//            evaluatorOptionValue = getDefaultPerformanceEvaluatorForLearner(classifier);
//        }
//        evaluator = new EvaluatorProcessor.Builder(evaluatorOptionValue)
//                .samplingFrequency(sampleFrequencyOption.getValue()).dumpFile(dumpFileOption.getFile()).build();

        ClassificationEvaluationProcessor evaluator=new ClassificationEvaluationProcessor("Result check");
        evaluator.setNumClasses(this.numClasses);
        //evaluator.setSamoaClassifiers(this.samoaClassifiers);
        // evaluatorPi = builder.createPi(evaluator);
        // evaluatorPi.connectInputShuffleStream(evaluatorPiInputStream);
        builder.addProcessor(evaluator);
        for (Stream evaluatorPiInputStream : classifier.getResultStreams()) {
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

    public void setNumClasses(int numClasses) {
        this.numClasses = numClasses;
    }
}