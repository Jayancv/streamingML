package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.moa.options.AbstractOptionHandler;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.StreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Vector;
import java.util.concurrent.*;

/**
 * Created by wso2123 on 10/11/16.
 */
public class StreamingRegressionEntranceProcessor implements EntranceProcessor {

    private static final long serialVersionUID = 4169053337917578558L;

    private static final Logger logger = LoggerFactory.getLogger(StreamingRegressionEntranceProcessor.class);


    private StreamSource streamSource;
    private Instance firstInstance;
    private boolean isInited = false;

    private int numberInstances;
    private int numInstanceSent = 0;

    protected InstanceStream sourceStream;

    /*
     * ScheduledExecutorService to schedule sending events after each delay
     * interval. It is expected to have only one event in the queue at a time, so
     * we need only one thread in the pool.
     */
    private transient ScheduledExecutorService timer;
    private transient ScheduledFuture<?> schedule = null;
    private int readyEventIndex = 1; // No waiting for the first event
    private int delay = 0;
    private int batchSize = 1;
    private boolean finished = false;

    String evalPoint;

    public ConcurrentLinkedQueue<Vector> predictions;


    public void setSamoaClassifiers(ConcurrentLinkedQueue<Vector> samoaPredictions) {
        this.predictions = samoaPredictions;
    }


    @Override
    public boolean process(ContentEvent event) {
        // TODO: possible refactor of the super-interface implementation
        // of source processor does not need this method
        return false;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public boolean hasNext() {
        return !isFinished() && (delay <= 0 || numInstanceSent < readyEventIndex);
    }

    private boolean hasReachedEndOfStream() {
        return (!streamSource.hasMoreInstances() || (numberInstances >= 0 && numInstanceSent >= numberInstances));
    }

    @Override
    public ContentEvent nextEvent() {
        InstanceContentEvent contentEvent = null;
        if (hasReachedEndOfStream()) {
            contentEvent = new InstanceContentEvent(-1, firstInstance, false, true);
            contentEvent.setLast(true);
            // set finished status _after_ tagging last event
            finished = true;
        } else if (hasNext()) {
            numInstanceSent++;
            contentEvent = new InstanceContentEvent(numInstanceSent, nextInstance(), true, true);

            // first call to this method will trigger the timer
            if (schedule == null && delay > 0) {
                schedule = timer.scheduleWithFixedDelay(new DelayTimeoutHandler(this), delay, delay,
                        TimeUnit.MICROSECONDS);
            }
        }
        return contentEvent;
    }

    private void increaseReadyEventIndex() {
        readyEventIndex += batchSize;
        // if we exceed the max, cancel the timer
        if (schedule != null && isFinished()) {
            schedule.cancel(false);
        }
    }

    @Override
    public void onCreate(int id) {
        initStreamSource(sourceStream);
        timer = Executors.newScheduledThreadPool(1);
        logger.debug("Creating PrequentialSourceProcessor with id {}", id);
    }

    @Override
    public Processor newProcessor(Processor p) {
        StreamingRegressionEntranceProcessor newProcessor = new StreamingRegressionEntranceProcessor();
        StreamingRegressionEntranceProcessor originProcessor = (StreamingRegressionEntranceProcessor) p;
        if (originProcessor.getStreamSource() != null) {
            newProcessor.setStreamSource(originProcessor.getStreamSource().getStream());
        }
        return newProcessor;
    }


    public StreamSource getStreamSource() {
        return streamSource;
    }

    public void setStreamSource(InstanceStream stream) {
        if (stream instanceof AbstractOptionHandler) {
            ((AbstractOptionHandler) (stream)).prepareForUse();
        }

        this.streamSource = new StreamSource(stream);
        firstInstance = streamSource.nextInstance().getData();
    }

    public Instances getDataset() {
        if (firstInstance == null) {
            initStreamSource(sourceStream);
        }
        return firstInstance.dataset();
    }

    private Instance nextInstance() {
        if (this.isInited) {
            return streamSource.nextInstance().getData();
        } else {
            this.isInited = true;
            return firstInstance;
        }
    }

    // private void sendEndEvaluationInstance(Stream inputStream) {
    // InstanceContentEvent contentEvent = new InstanceContentEvent(-1,
    // firstInstance, false, true);
    // contentEvent.setLast(true);
    // inputStream.put(contentEvent);
    // }

    private void initStreamSource(InstanceStream stream) {
        if (stream instanceof AbstractOptionHandler) {
            ((AbstractOptionHandler) (stream)).prepareForUse();
        }

        this.streamSource = new StreamSource(stream);
        firstInstance = streamSource.nextInstance().getData();
    }

    public void setMaxNumInstances(int value) {
        numberInstances = value;
    }

    public int getMaxNumInstances() {
        return this.numberInstances;
    }

    public void setSourceDelay(int delay) {
        this.delay = delay;
    }

    public int getSourceDelay() {
        return this.delay;
    }

    public void setDelayBatchSize(int batch) {
        this.batchSize = batch;
    }

    private class DelayTimeoutHandler implements Runnable {

        private StreamingRegressionEntranceProcessor processor;

        public DelayTimeoutHandler(StreamingRegressionEntranceProcessor processor) {
            this.processor = processor;
        }

        public void run() {
            processor.increaseReadyEventIndex();
        }
    }
}
