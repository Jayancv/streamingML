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
            File f = new File("iris.csv");
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            scn = new Scanner(br);

        }catch (Exception e){
            logger.info(e.toString());
        }
        int learnType = 0;
        int paramCount = 5;               //number of attributes
        int batchSize = 1000;
        int numClasses = 3;

        Classification classification = new Classification(learnType,paramCount, batchSize,paramCount);

        new Thread(classification).start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Successfully Instatiated the Clustering with samoa");

        double [] cepEvent=new double[paramCount];

        while(true){
            Object[] outputData = null;

            if(scn.hasNext()) {
                String eventStr = scn.nextLine();
                String[] event=eventStr.split(",");
                event[paramCount] = event[paramCount].substring(1,event[paramCount].length()-1);
                if(event[paramCount].equals("setosa")){
                    cepEvent[paramCount-1] = 0.0D;
                } else if(event[paramCount].equals("virginica")){
                    cepEvent[paramCount-1] = 1.0D;
                }else if(event[paramCount].equals("versicolor")){
                    cepEvent[paramCount-1] = 2.0D;
                }
                for(int i=0;i<paramCount-1;i++){
                    cepEvent[i]=Double.parseDouble(event[i+1]);

                }
                outputData = classification.classify(cepEvent);

                if (outputData == null) {
                    // System.out.println("null");
                } else {
                    System.out.println("Godaaa");
//                    for (int i = 0; i < numClusters; i++) {
//                        System.out.println("center " + i + ": " + outputData[i + 1]);
//                    }
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

//package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
//import java.util.Scanner;
//
///**
// * Created by wso2123 on 9/2/16.
// */
//public class Simulator {
//    private static final Logger logger = LoggerFactory.getLogger(Simulator.class);
//
//    public static Scanner scn;
//    public static void main(String[] args){
//        System.out.println("Starts");
//        try {
//            File f = new File("ccpp.csv");
//            FileReader fr = new FileReader(f);
//            BufferedReader br = new BufferedReader(fr);
//            scn = new Scanner(br);
//
//        }catch (Exception e){
//            logger.info(e.toString());
//        }
//        int learnType = 0;
//        int paramCount = 5;               //number of attributes
//        int batchSize = 1000;
//        int numClasses = 2;
//
//
//        Classification classification = new Classification(learnType,paramCount, batchSize,paramCount);
//
//        new Thread(classification).start();
//
//        try {
//            Thread.sleep(1000
//            );
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        logger.info("Successfully Instatiated the classification with samoa");
//
//        double [] cepEvent=new double[paramCount];
//
//        while(true){
//            Object[] outputData = null;
//
//            if(scn.hasNext()) {
//                String eventStr = scn.nextLine();
//                String[] event=eventStr.split(",");
//                for(int i=0;i<paramCount;i++){
//                    cepEvent[i]=Double.parseDouble(event[i]);
//
//                }
//                outputData = classification.classify(cepEvent);
//
//                if (outputData == null) {
//                    // System.out.println("null");
//                } else {
//                    System.out.println("Godaaa");
////                    for (int i = 0; i < numClusters; i++) {
////                        System.out.println("center " + i + ": " + outputData[i + 1]);
////                    }
//                }
//            }
//
//            // System.out.println(cepEvent[2]);
//
//
//            try {
//                Thread.sleep(1);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//
//        }
//    }
//}
