package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression;

/**
 * Created by wso2123 on 10/11/16.
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.evaluation.PerformanceEvaluator;
import org.apache.samoa.instances.Utils;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.evaluation.LearningCurve;
import org.apache.samoa.moa.evaluation.LearningEvaluation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingRegressionEvaluationProcessor implements Processor {

    /**
     *
     */
    private static final long serialVersionUID = -2778051819116753612L;
    private static final Logger logger = LoggerFactory.getLogger(StreamingRegressionEvaluationProcessor.class);
    private static final String ORDERING_MEASUREMENT_NAME = "evaluation instances";
    private final PerformanceEvaluator evaluator;
    private final int samplingFrequency;
    private final File dumpFile;
    private transient PrintStream immediateResultStream = null;
    private transient boolean firstDump = true;
    private long totalCount = 0;
    private long experimentStart = 0;
    private long sampleStart = 0;
    private LearningCurve learningCurve;
    private int id;


    public ConcurrentLinkedQueue<Vector> regressionData;
    private ArrayList<Integer> predictions= new ArrayList<Integer>();

    public StreamingRegressionEvaluationProcessor(StreamingRegressionEvaluationProcessor.Builder builder) {
        this.immediateResultStream = null;
        this.firstDump = true;
        this.totalCount = 0L;
        this.experimentStart = 0L;
        this.sampleStart = 0L;
        this.evaluator = builder.evaluator;
        this.samplingFrequency = builder.samplingFrequency;
        this.dumpFile = builder.dumpFile;
    }

    @Override
    public boolean process(ContentEvent event) {

        ResultContentEvent result = (ResultContentEvent) event;
        if ((totalCount > 0) && (totalCount % samplingFrequency) == 0) {
            long sampleEnd = System.nanoTime();
            long sampleDuration = TimeUnit.SECONDS.convert(sampleEnd - sampleStart, TimeUnit.NANOSECONDS);
            sampleStart = sampleEnd;

            logger.info("{} seconds for {} instances", sampleDuration, samplingFrequency);
            this.addMeasurement();
        }

        if (result.isLastEvent()) {
            this.concludeMeasurement();
            return true;
        }

        evaluator.addResult(result.getInstance(), result.getClassVotes());
        totalCount += 1;

        int pre= Utils.maxIndex(result.getClassVotes());
        predictions.add(pre);

        if (totalCount == 1) {
            sampleStart = System.nanoTime();
            experimentStart = sampleStart;
        }

        return false;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        this.learningCurve = new LearningCurve(ORDERING_MEASUREMENT_NAME);

        if (this.dumpFile != null) {
            try {
                if (dumpFile.exists()) {
                    this.immediateResultStream = new PrintStream(
                            new FileOutputStream(dumpFile, true), true);
                } else {
                    this.immediateResultStream = new PrintStream(
                            new FileOutputStream(dumpFile), true);
                }

            } catch (FileNotFoundException e) {
                this.immediateResultStream = null;
                logger.error("File not found exception for {}:{}", this.dumpFile.getAbsolutePath(), e.toString());

            } catch (Exception e) {
                this.immediateResultStream = null;
                logger.error("Exception when creating {}:{}", this.dumpFile.getAbsolutePath(), e.toString());
            }
        }

        this.firstDump = true;
    }

    @Override
    public Processor newProcessor(Processor p) {
        StreamingRegressionEvaluationProcessor originalProcessor = (StreamingRegressionEvaluationProcessor) p;
        StreamingRegressionEvaluationProcessor newProcessor =( new StreamingRegressionEvaluationProcessor.Builder(originalProcessor)).build();

        if (originalProcessor.learningCurve != null) {
            newProcessor.learningCurve = originalProcessor.learningCurve;
        }

        return newProcessor;
    }

    @Override
    public String toString() {
        StringBuilder report = new StringBuilder();

        report.append(StreamingRegressionEvaluationProcessor.class.getCanonicalName());
        report.append("id = ").append(this.id);
        report.append('\n');
        if (learningCurve.numEntries() > 0) {
            report.append(learningCurve.toString());
            report.append('\n');
        }
        return report.toString();
    }

    private void addMeasurement() {
        Vector measurements = new Vector<>();
        measurements.add(new Measurement(ORDERING_MEASUREMENT_NAME, totalCount));
        Collections.addAll(measurements, evaluator.getPerformanceMeasurements());
        Measurement[] finalMeasurements = (Measurement[]) measurements.toArray(new Measurement[measurements.size()]);
        LearningEvaluation learningEvaluation = new LearningEvaluation(finalMeasurements);
        learningCurve.insertEntry(learningEvaluation);
        logger.debug("evaluator id = {}", this.id);
        logger.info(learningEvaluation.toString());

        try{
            regressionData.add(measurements);
        }catch (Exception e){
            logger.info(e.getMessage());
        }

        if (immediateResultStream != null) {
            if (firstDump) {
                immediateResultStream.println(learningCurve.headerToString());
                firstDump = false;
            }

            immediateResultStream.println(learningCurve.entryToString(learningCurve.numEntries() - 1));
            immediateResultStream.flush();
        }
    }

    private void concludeMeasurement() {
        logger.info("last event is received!");
        logger.info("total count: {}", this.totalCount);
        String learningCurveSummary = this.toString();
        logger.info(learningCurveSummary);
        long experimentEnd = System.nanoTime();
        long totalExperimentTime = TimeUnit.SECONDS.convert(experimentEnd - experimentStart, TimeUnit.NANOSECONDS);
        logger.info("total evaluation time: {} seconds for {} instances", totalExperimentTime, totalCount);

        if (immediateResultStream != null) {
            immediateResultStream.println("# COMPLETED");
            immediateResultStream.flush();
        }
        // logger.info("average throughput rate: {} instances/seconds",
        // (totalCount/totalExperimentTime));
    }

    public void setPredictions(ArrayList<Integer> predictions) {
        this.predictions = predictions;
    }

    public static class Builder {

        private final PerformanceEvaluator evaluator;
        private int samplingFrequency = 100000;
        private File dumpFile = null;

        public Builder(PerformanceEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        public Builder(StreamingRegressionEvaluationProcessor oldProcessor) {
            this.evaluator = oldProcessor.evaluator;
            this.samplingFrequency = oldProcessor.samplingFrequency;
            this.dumpFile = oldProcessor.dumpFile;
        }

        public Builder samplingFrequency(int samplingFrequency) {
            this.samplingFrequency = samplingFrequency;
            return this;
        }

        public Builder dumpFile(File file) {
            this.dumpFile = file;
            return this;
        }

        public StreamingRegressionEvaluationProcessor build() {
            return new StreamingRegressionEvaluationProcessor(this);
        }
    }
}