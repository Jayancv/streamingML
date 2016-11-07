package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.evaluation.PerformanceEvaluator;
import org.apache.samoa.instances.Utils;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.evaluation.LearningCurve;
import org.apache.samoa.moa.evaluation.LearningEvaluation;
import org.apache.spark.sql.catalyst.expressions.In;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by wso2123 on 8/30/16.
 */
public class StreamingClassificationEvaluationProcessor implements Processor {

    private static final long serialVersionUID = -2778051819116753612L;
    private static final Logger logger = LoggerFactory.getLogger(StreamingClassificationEvaluationProcessor.class);
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

    public ConcurrentLinkedQueue<Vector> statistics = new ConcurrentLinkedQueue<>();
    public ConcurrentLinkedQueue<Vector> classifiers;

    private int count = 0;


    private StreamingClassificationEvaluationProcessor(StreamingClassificationEvaluationProcessor.Builder builder) {

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
        boolean predicting = false;
        ResultContentEvent result = (ResultContentEvent) event;
        count++;

        if (result.getInstance().classValue() == -1) {          // Identify the event that uses to predict or train
            predicting = true;
        }

        if (this.totalCount > 0L && this.totalCount % (long) this.samplingFrequency == 0L && !predicting) {            // After every interval log the current statistics
            long sampleEnd = System.nanoTime();
            long sampleDuration = TimeUnit.SECONDS.convert(sampleEnd - this.sampleStart, TimeUnit.NANOSECONDS);
            this.sampleStart = sampleEnd;
            this.addMeasurement();                                                                                     //calculate measurements
            if (!statistics.isEmpty()) {
                Vector stat = statistics.poll();
                System.out.println(stat.toString());
            }
        }

        if (result.isLastEvent()) {
            this.concludeMeasurement();
            return true;
        } else {
            int pre = Utils.maxIndex(result.getClassVotes());
            if (predicting) {
                Vector t = new Vector();
                for (int i = 0; i < result.getInstance().numValues() - 1; i++) {                   // add event attribute to vector t and add that vector to samoaClassifier
                    t.add(result.getInstance().value(i));
                }
                t.add(pre);
                classifiers.add(t);

                //  System.out.println(pre +" total "+ totalCount);
            } else {                                                         // Training event data added to model statistics
                this.evaluator.addResult(result.getInstance(), result.getClassVotes());
                ++this.totalCount;
            }
            if (this.totalCount == 1L) {
                this.sampleStart = System.nanoTime();
                this.experimentStart = this.sampleStart;
            }

            return true;
        }


    }

    public void onCreate(int id) {
        this.id = id;
        this.learningCurve = new LearningCurve("evaluation instances");
        if (this.dumpFile != null) {
            try {
                if (this.dumpFile.exists()) {
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

    public void setSamoaClassifiers(ConcurrentLinkedQueue<Vector> classifiers) {
        this.classifiers = classifiers;

    }

    public Processor newProcessor(Processor p) {
        StreamingClassificationEvaluationProcessor originalProcessor = (StreamingClassificationEvaluationProcessor) p;
        StreamingClassificationEvaluationProcessor newProcessor = (new StreamingClassificationEvaluationProcessor.Builder(originalProcessor)).build();
        newProcessor.setSamoaClassifiers(classifiers);
        if (originalProcessor.learningCurve != null) {
            newProcessor.learningCurve = originalProcessor.learningCurve;
        }

        return newProcessor;
    }

    public String toString() {
        StringBuilder report = new StringBuilder();
        report.append(StreamingClassificationEvaluationProcessor.class.getCanonicalName());
        report.append("id = ").append(this.id);
        report.append('\n');
        if (this.learningCurve.numEntries() > 0) {
            report.append(this.learningCurve.toString());
            report.append('\n');
        }

        return report.toString();
    }

    private void addMeasurement() {
        Vector measurements = new Vector();
        measurements.add(new Measurement("evaluation instances", (double) this.totalCount));
        Collections.addAll(measurements, this.evaluator.getPerformanceMeasurements());
        Measurement[] finalMeasurements = (Measurement[]) measurements.toArray(new Measurement[measurements.size()]);
        LearningEvaluation learningEvaluation = new LearningEvaluation(finalMeasurements);
        this.learningCurve.insertEntry(learningEvaluation);
        try {
            statistics.add(measurements);
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
        ;
        if (this.immediateResultStream != null) {
            if (this.firstDump) {
                this.immediateResultStream.println(this.learningCurve.headerToString());
                //  logger.info("checked");
                this.firstDump = false;
            }

            this.immediateResultStream.println(this.learningCurve.entryToString(this.learningCurve.numEntries() - 1));
            logger.info("checked");
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
        if (this.immediateResultStream != null) {
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

        public Builder(StreamingClassificationEvaluationProcessor oldProcessor) {
            this.evaluator = oldProcessor.evaluator;
            this.samplingFrequency = oldProcessor.samplingFrequency;
            this.dumpFile = oldProcessor.dumpFile;
        }

        public StreamingClassificationEvaluationProcessor.Builder samplingFrequency(int samplingFrequency) {
            this.samplingFrequency = samplingFrequency;
            return this;
        }

        public StreamingClassificationEvaluationProcessor.Builder dumpFile(File file) {
            this.dumpFile = file;
            return this;
        }

        public StreamingClassificationEvaluationProcessor build() {
            return new StreamingClassificationEvaluationProcessor(this);
        }


    }
}
