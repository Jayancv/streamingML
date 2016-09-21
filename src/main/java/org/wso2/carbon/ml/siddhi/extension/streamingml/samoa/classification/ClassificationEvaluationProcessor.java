package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
//import org.apache.samoa.evaluation.EvaluatorProcessor;
import org.apache.samoa.evaluation.PerformanceEvaluator;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.classifiers.AbstractClassifier;
import org.apache.samoa.moa.cluster.Clustering;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.evaluation.LearningCurve;
import org.apache.samoa.moa.evaluation.LearningEvaluation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by wso2123 on 8/30/16.
 */
public class ClassificationEvaluationProcessor implements Processor {

    private static final long serialVersionUID = -2778051819116753612L;
    private static final Logger logger = LoggerFactory.getLogger(ClassificationEvaluationProcessor.class);
    private static final String ORDERING_MEASUREMENT_NAME = "evaluation instances";
    private final PerformanceEvaluator evaluator;
    private final int samplingFrequency;
    private final File dumpFile;
    private transient PrintStream immediateResultStream;
    private transient boolean firstDump;
    private long totalCount;
    private long experimentStart;
    private long sampleStart;
    private LearningCurve learningCurve;
    private int id;

    private ClassificationEvaluationProcessor(ClassificationEvaluationProcessor.Builder builder) {

        this.immediateResultStream = null;
        this.firstDump = true;
        this.totalCount = 0L;
        this.experimentStart = 0L;
        this.sampleStart = 0L;
        this.evaluator = builder.evaluator;
        this.samplingFrequency = builder.samplingFrequency;
        this.dumpFile = builder.dumpFile;
    }


    public boolean process(ContentEvent event) {
        ResultContentEvent result = (ResultContentEvent)event;
        if(this.totalCount > 0L && this.totalCount % (long)this.samplingFrequency == 0L) {
            long sampleEnd = System.nanoTime();
            long sampleDuration = TimeUnit.SECONDS.convert(sampleEnd - this.sampleStart, TimeUnit.NANOSECONDS);
            this.sampleStart = sampleEnd;
            logger.info("{} seconds for {} instances", Long.valueOf(sampleDuration), Integer.valueOf(this.samplingFrequency));
            this.addMeasurement();
        }

        if(result.isLastEvent()) {
            this.concludeMeasurement();
            return true;
        } else {
            this.evaluator.addResult(result.getInstance(), result.getClassVotes());
            ++this.totalCount;
            if(this.totalCount == 1L) {
                this.sampleStart = System.nanoTime();
                this.experimentStart = this.sampleStart;
            }

            return false;
        }
    }

    public void onCreate(int id) {
        this.id = id;
        this.learningCurve = new LearningCurve("evaluation instances");
        if(this.dumpFile != null) {
            try {
                if(this.dumpFile.exists()) {
                    this.immediateResultStream = new PrintStream(new FileOutputStream(this.dumpFile, true), true);
                } else {
                    this.immediateResultStream = new PrintStream(new FileOutputStream(this.dumpFile), true);
                }
            } catch (FileNotFoundException var3) {
                this.immediateResultStream = null;
                logger.error("File not found exception for {}:{}", this.dumpFile.getAbsolutePath(), var3.toString());
            } catch (Exception var4) {
                this.immediateResultStream = null;
                logger.error("Exception when creating {}:{}", this.dumpFile.getAbsolutePath(), var4.toString());
            }
        }

        this.firstDump = true;
    }

    public Processor newProcessor(Processor p) {
        ClassificationEvaluationProcessor originalProcessor = (ClassificationEvaluationProcessor)p;
        ClassificationEvaluationProcessor newProcessor = (new ClassificationEvaluationProcessor.Builder(originalProcessor)).build();
        if(originalProcessor.learningCurve != null) {
            newProcessor.learningCurve = originalProcessor.learningCurve;
        }

        return newProcessor;
    }

    public String toString() {
        StringBuilder report = new StringBuilder();
        report.append(ClassificationEvaluationProcessor.class.getCanonicalName());
        report.append("id = ").append(this.id);
        report.append('\n');
        if(this.learningCurve.numEntries() > 0) {
            report.append(this.learningCurve.toString());
            report.append('\n');
        }

        return report.toString();
    }

    private void addMeasurement() {
        Vector measurements = new Vector();
        measurements.add(new Measurement("evaluation instances", (double)this.totalCount));
        Collections.addAll(measurements, this.evaluator.getPerformanceMeasurements());
        Measurement[] finalMeasurements = (Measurement[])measurements.toArray(new Measurement[measurements.size()]);
        LearningEvaluation learningEvaluation = new LearningEvaluation(finalMeasurements);
        this.learningCurve.insertEntry(learningEvaluation);
        logger.debug("evaluator id = {}", Integer.valueOf(this.id));
        logger.info(learningEvaluation.toString());
        if(this.immediateResultStream != null) {
            if(this.firstDump) {
                this.immediateResultStream.println(this.learningCurve.headerToString());
                this.firstDump = false;
            }

            this.immediateResultStream.println(this.learningCurve.entryToString(this.learningCurve.numEntries() - 1));
            this.immediateResultStream.flush();
        }

    }

    private void concludeMeasurement() {
        logger.info("last event is received!");
        logger.info("total count: {}", Long.valueOf(this.totalCount));
        String learningCurveSummary = this.toString();
        logger.info(learningCurveSummary);
        long experimentEnd = System.nanoTime();
        long totalExperimentTime = TimeUnit.SECONDS.convert(experimentEnd - this.experimentStart, TimeUnit.NANOSECONDS);
        logger.info("total evaluation time: {} seconds for {} instances", Long.valueOf(totalExperimentTime), Long.valueOf(this.totalCount));
        if(this.immediateResultStream != null) {
            this.immediateResultStream.println("# COMPLETED");
            this.immediateResultStream.flush();
        }

    }

    public static class Builder {
        private final PerformanceEvaluator evaluator;
        private int samplingFrequency = 100000;
        private File dumpFile = null;

        public Builder(PerformanceEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        public Builder(ClassificationEvaluationProcessor oldProcessor) {
            this.evaluator = oldProcessor.evaluator;
            this.samplingFrequency = oldProcessor.samplingFrequency;
            this.dumpFile = oldProcessor.dumpFile;
        }

        public ClassificationEvaluationProcessor.Builder samplingFrequency(int samplingFrequency) {
            this.samplingFrequency = samplingFrequency;
            return this;
        }

        public ClassificationEvaluationProcessor.Builder dumpFile(File file) {
            this.dumpFile = file;
            return this;
        }

        public ClassificationEvaluationProcessor build() {
            return new ClassificationEvaluationProcessor(this);
        }
    }

//    private static final long serialVersionUID = -6043613438148776446L;
//    private int processorId;
//    private static final Logger logger = LoggerFactory.getLogger(ClassificationEvaluationProcessor.class);
//
//    String evalPoint;
//    //public LinkedList<Clustering>samoaClusters;
//    //public ConcurrentLinkedQueue<double[]> cepEvents;
//   // public ConcurrentLinkedQueue<AbstractClassifier> samoaClassifiers;
//    public int numClasses=0;
//    private ConcurrentLinkedQueue<AbstractClassifier> samoaClassifiers;
//
//    public ClassificationEvaluationProcessor(String s) {
//        this.evalPoint=s;
//    }
//
//    @Override
//    public boolean process(ContentEvent event) {
//
//        if (event instanceof InstanceContentEvent) {
//            logger.info(event.getKey()+""+evalPoint+"ClusteringContentEvent");
//            InstanceContentEvent e= (InstanceContentEvent) event;
//            Instance inst = e.getInstance();
//
//            int numAttributes=inst.numAttributes();
//        }
//
//        else if(event instanceof ResultContentEvent){
//            logger.info(event.getKey()+" "+evalPoint+" ClusteringResultContentEvent "+numClasses);
//            ResultContentEvent resultEvent = (ResultContentEvent) event;
//
//          // Clustering clustering=resultEvent.getClustering();
//
//           // Clustering kmeansClustering = WithKmeans.kMeans_rand(numClusters,clustering);
//           // logger.info("Kmean Clusters: "+kmeansClustering.size()+" with dimention of : "+kmeansClustering.dimension());
//            //Adding samoa Clusters into my class
//            //samoaClusters.add(kmeansClustering);
//
//            //int numClusters = clustering.size();
//            //logger.info("Number of Kernal Clusters : "+numClusters+" Number of KMeans Clusters :"+kmeansClustering.size());
//         //AbstractClassifier classifier=resultEvent.ge;
//
//        }
//
////        else if(event instanceof ClusteringEvaluationContentEvent){
////            logger.info(event.getKey()+""+evalPoint+"ClusteringEvaluationContentEvent\n");
////        }
////        else{
////            logger.info(event.getKey()+""+evalPoint+"ContentEvent\n");
////        }
//
//        return true;
//    }
//
//    @Override
//    public void onCreate(int i) {
//
//    }
//
//    @Override
//    public Processor newProcessor(Processor processor) {
//        return null;
//    }
//
//
//
//    public void setNumClasses(int numClasses) {
//        this.numClasses = numClasses;
//    }
//
//    public void setSamoaClassifiers(ConcurrentLinkedQueue<AbstractClassifier> samoaClassifiers) {
//        this.samoaClassifiers = samoaClassifiers;
//    }
}
