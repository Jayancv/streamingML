package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;

/**
 * Created by wso2123 on 8/30/16.
 */
public class ClassificationEvaluationProcessor implements Processor {
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
}
