package org.wso2.carbon.ml.siddhi.extension.streamingml;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;

public class AppTestCase {
    private static final Logger logger = Logger.getLogger(AppTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testClassificationtreamProcessorExtension() throws InterruptedException {
        logger.info("StreamingClasificationStreamProcessor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream inputStream (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from inputStream#streamingml:streamclassification(1000000, 1,3,5,0,\"\",1,0, attribute_0, attribute_1 , attribute_2 , attribute_3 , attribute_4) " +
                "insert all events into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
            }
        });

        try {
            Scanner scn;
            File f = new File("iris.csv");
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            scn = new Scanner(br);

            InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
            executionPlanRuntime.start();
            while (true) {
                if (scn.hasNext()) {
                    String eventStr = scn.nextLine();
                    String[] event = eventStr.split(",");

                    inputHandler.send(new Object[]{Double.valueOf(event[0]), Double.valueOf(event[1]), Double.valueOf(event[2]), Double.valueOf(event[3]), event[4]});


                } else {
                    break;
                }
            }
            Thread.sleep(1100);
          //  Assert.assertEquals(2, count);
            Assert.assertTrue(eventArrived);
            executionPlanRuntime.shutdown();
        } catch (Exception e) {
            logger.info(e.toString());
        }


    }
}