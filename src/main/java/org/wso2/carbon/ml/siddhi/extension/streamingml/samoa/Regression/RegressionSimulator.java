package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Regression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;

/**
 * Created by wso2123 on 10/11/16.
 */
public class RegressionSimulator {
    private static final Logger logger = LoggerFactory.getLogger(RegressionSimulator.class);
    public static Scanner scn;

    public static void main(String[] args) {
        System.out.println("Starts");
        try {
            File f = new File("CASP.csv");
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            scn = new Scanner(br);

        } catch (Exception e) {
            logger.info(e.toString());
        }

        int maxInstance = 1000000;
        int batchSize = 5000;
        int paramCount = 10;
        int parallelism = 1;
        int numModelsBagging = 0;
        int numInstant = 0;
        StreamingRegression streamingRegression = new StreamingRegression(maxInstance, batchSize, paramCount, parallelism, numModelsBagging);

        new Thread(streamingRegression).start();                     // Start new regression thread

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Successfully Instantiated the Regression with samoa");

        double[] cepEvent = new double[paramCount];
        while (true) {
            Object[] outputData = null;
            if (scn.hasNext()) {
                numInstant++;
                String eventStr = scn.nextLine();
                String[] event = eventStr.split(",");
                for (int i = 0; i < paramCount; i++) {
                    cepEvent[i] = Double.parseDouble(event[i]);
                }
                outputData = streamingRegression.regress(cepEvent);            // add cepEvent to cepevent concurrentQueue and get output

            } else {
                System.out.println(numInstant + " events added from CEP");
                break;
            }

            try {
                Thread.sleep(1);                        // Delay between two events
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
