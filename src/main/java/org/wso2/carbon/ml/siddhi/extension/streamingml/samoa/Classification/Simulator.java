package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.Classification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by wso2123 on 9/2/16.
 */
public class Simulator {
    private static final Logger logger = LoggerFactory.getLogger(Simulator.class);

    public static Scanner scn;

    public static void main(String[] args) {
        System.out.println("Starts");
        try {
            File f = new File("test1.csv");
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            scn = new Scanner(br);

        } catch (Exception e) {
            logger.info(e.toString());
        }
        int learnType = 0;
        int maxinstance = 100000;
        int batchSize = 1000;
        int numClasses = 4;                    //Number of classes
        int paramCount = 19;                    //Number of all attributes with numeric,nominal and class
        int nominalOption =15 ;                 //Number of nominal attributes without class attribute
        String nominalAttributeValues = "2,2,2,2,2,2,2,2,2,2,2,2,2,2,3,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,3";    //A string that contain number of values that each nominal attribute has

        StreamingClassification streamingClassification = new StreamingClassification(maxinstance, batchSize, numClasses, paramCount, nominalOption,nominalAttributeValues);

        new Thread(streamingClassification).start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        double[] cepEvent = new double[paramCount];          //Event comes from CEP
        List<String> classes = new ArrayList<String>();           //values of class attribute
        ArrayList<ArrayList<String>> nominals = new ArrayList<ArrayList<String>>();     //values of other nominal attributes
        while (true) {
            Object[] outputData = null;

            if (scn.hasNext()) {
                String eventStr = scn.nextLine();
                String[] event = eventStr.split(",");
                int k = numClasses - 1;
                String classValue = event[(paramCount - 1)];

                if (classes.contains(classValue)) {
                    cepEvent[paramCount - 1] = classes.indexOf(classValue);
                } else {
                    classes.add(classValue);
                    cepEvent[paramCount - 1] = classes.indexOf(classValue);
                }
                int j =0;

                for (int i = 0; i < paramCount - 1; i++) {

                    if(i<paramCount-1-nominalOption) {
                        cepEvent[i] = Double.parseDouble(event[i]);
                    }else{
                        String v= event[i];
                        try {
                            if (!nominals.get(j).contains(event[i])) {
                                nominals.get(j).add(v);
                            }
                        }catch (IndexOutOfBoundsException e){
                            nominals.add(new ArrayList<String>());
                            nominals.get(j).add(v);
                        }
                        cepEvent[i]=(nominals.get(j).indexOf(event[i]));
                        j++;
                    }

                }
                outputData = streamingClassification.classify(cepEvent);

//                if (outputData == null) {
//                    System.out.println("null");
//                } else {
//
//                    for (int i = 0; i < 4; i++) {
//                        System.out.println( outputData[i]);
//                    }
//                }
            }


            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
    }
}

