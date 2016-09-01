package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import com.github.javacliparser.*;
import org.apache.samoa.moa.MOAObject;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.tasks.Task;
import org.apache.samoa.moa.tasks.TaskMonitor;

/**
 * Created by wso2123 on 8/30/16.
 */
public class ClassificationTask implements Task, Configurable {
    @Override
    public Class<?> getTaskResultType() {
        return null;
    }

    @Override
    public Object doTask() {
        return null;
    }

    @Override
    public Object doTask(TaskMonitor taskMonitor, ObjectRepository objectRepository) {
        return null;
    }

    @Override
    public int measureByteSize() {
        return 0;
    }

    @Override
    public MOAObject copy() {
        return null;
    }

    @Override
    public void getDescription(StringBuilder stringBuilder, int i) {

    }
}
