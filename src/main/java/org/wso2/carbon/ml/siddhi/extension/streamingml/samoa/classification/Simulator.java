package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;

/**
 * Created by wso2123 on 9/2/16.
 */
public class Simulator {
    private static final Logger logger = LoggerFactory.getLogger(Simulator.class);

    public static Scanner scn;
    public static void main(String[] args){
        System.out.println("Starts");
        try {
            File f = new File("ccpp.csv");
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            scn = new Scanner(br);

        }catch (Exception e){
            logger.info(e.toString());
        }
        int learnType = 0;
        int paramCount = 5;
        int batchSize = 1000;
        int numClusters = 2;

        Classification classification = new Classification(learnType,paramCount, batchSize);

        new Thread(classification).start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Successfully Instatiated the Clustering with samoa");

        double [] cepEvent=new double[paramCount];
       /*for(int i=0;i<paramCount;i++){
            cepEvent[i]=(int)(Math.random()*100);

        }*/
        while(true){
            Object[] outputData = null;
            //logger.info("Sending Next Event"+numInsancesSent++);
            // Object[] outputData= streamingLinearRegression.addToRDD(eventData);
            //Calling the regress function
            if(scn.hasNext()) {
                String eventStr = scn.nextLine();
                String[] event=eventStr.split(",");
                for(int i=0;i<paramCount;i++){
                    cepEvent[i]=Double.parseDouble(event[i]);

                }
                outputData = classification.classify(cepEvent);

                if (outputData == null) {
                    // System.out.println("null");
                } else {
                    System.out.println("Error: " + outputData[0]);
                    for (int i = 0; i < numClusters; i++) {
                        System.out.println("center " + i + ": " + outputData[i + 1]);
                    }
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
