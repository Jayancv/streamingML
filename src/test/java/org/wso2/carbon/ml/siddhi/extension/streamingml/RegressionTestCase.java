/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.wso2.carbon.ml.siddhi.extension.streamingml;

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

public class RegressionTestCase {
    private static final Logger logger = Logger.getLogger(RegressionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testRegressionStreamProcessorExtension() throws InterruptedException {
        logger.info("StreamingRegressionStreamProcessor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream inputStream (attribute_0 double, attribute_1 double, " +
                "attribute_2 double, attribute_3 double, attribute_4 double );";

        String query = ("@info(name = 'query2') from inputStream#streamingml:streamingRegressionSamoa(3000,500,5,4," +
                " attribute_0, attribute_1 , attribute_2 , attribute_3 , attribute_4) " +
                "select att_0 as arrtibute_0,att_1 as arrtibute_1,att_2 as arrtibute_2,att_3 as arrtibute_3, " +
                "prediction as prediction insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
            }
        });

        try {
            Scanner scn;
            File f = new File("ccpp.csv");
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            scn = new Scanner(br);

            InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
            executionPlanRuntime.start();
            while (true) {
                if (scn.hasNext()) {
                    String eventStr = scn.nextLine();
                    String[] event = eventStr.split(",");
                    inputHandler.send(new Object[]{Double.valueOf(event[0]), Double.valueOf(event[1]),
                            Double.valueOf(event[2]), Double.valueOf(event[3]), Double.valueOf(event[4])});
                } else {
                    break;
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }




            Thread.sleep(1100);
            //  Assert.assertEquals(2, count);
//            Assert.assertTrue(eventArrived);
            executionPlanRuntime.shutdown();
        } catch (Exception e) {
            logger.info(e.toString());
        }


    }
}