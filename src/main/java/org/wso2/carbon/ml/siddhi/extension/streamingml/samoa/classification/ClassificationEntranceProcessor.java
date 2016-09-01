package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;

/**
 * Created by wso2123 on 8/30/16.
 */
public class ClassificationEntranceProcessor implements EntranceProcessor {
    @Override
    public boolean process(ContentEvent contentEvent) {
        return false;
    }

    @Override
    public void onCreate(int i) {

    }

    @Override
    public Processor newProcessor(Processor processor) {
        return null;
    }

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public ContentEvent nextEvent() {
        return null;
    }
}
