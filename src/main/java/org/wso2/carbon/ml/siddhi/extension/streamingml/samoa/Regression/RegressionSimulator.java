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
            File f = new File("ccpp.csv");
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            scn = new Scanner(br);

        } catch (Exception e) {
            logger.info(e.toString());
        }

         int maxInstance = 1000000;
         int batchSize = 500;
         int paramCount = 5;
         int parallelism = 1;
         int numModelsBagging = 0;

        StreamingRegression streamingRegression = new StreamingRegression(maxInstance,paramCount, batchSize,parallelism,numModelsBagging );

        new Thread(streamingRegression).start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Successfully Instatiated the Clustering with samoa");

        double[] cepEvent = new double[paramCount];
       /*for(int i=0;i<paramCount;i++){
            cepEvent[i]=(int)(Math.random()*100);

        }*/
        while (true) {
            Object[] outputData = null;
            // logger.info("Sending Next Event"+numInsancesSent++);
            // Object[] outputData= streamingLinearRegression.addToRDD(eventData);
            //Calling the regress function
            if (scn.hasNext()) {
                String eventStr = scn.nextLine();
                String[] event = eventStr.split(",");
                for (int i = 0; i < paramCount; i++) {
                    cepEvent[i] = Double.parseDouble(event[i]);

                }
                outputData = streamingRegression.regress(cepEvent);

                if (outputData == null) {
                    //  System.out.println("null");
                } else {

                }
            }


            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
    }
}
