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
 * <p>
 * <p>
 * CEP Query
 * from inputStream#streamingml:streamclassification(maxInstance, displayInterval, numberOfClasses,NumberOfAllAttributes,NumberOfNominalAttributesWithoutClass,"numberOfValuesPerEachNominalAttribute", attribute_0, attribute_1 ,...........)
 * select *
 * insert into outputStream
 */


public class Simulator {
    private static final Logger logger = LoggerFactory.getLogger(Simulator.class);
    public static Scanner scn;

    public static void main(String[] args) {
        System.out.println("Starts");
        try {
            File f = new File("b.csv");
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            scn = new Scanner(br);

        } catch (Exception e) {
            logger.info(e.toString());
        }
        int maxinstance = 5000000;
        int batchSize = 500;                                           //Display interval
        int numClasses = 2;                                          //Number of classes
        int paramCount = 5;                                          //Number of all attributes with numeric,nominal and class
        int nominalOption = 0;                                       //Number of nominal attributes without class attribute
        String nominalAttributeValues = " ";
        int paralesum = 1;
        int bagging = 20;
        int numInstant = 0;

        StreamingClassification streamingClassification = new StreamingClassification(maxinstance, batchSize, numClasses, paramCount, nominalOption, nominalAttributeValues, paralesum, bagging);

        new Thread(streamingClassification).start();           //Start streamingClassification thread

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        double[] cepEvent = new double[paramCount];                      //Event comes from CEP
        List<String> classes = new ArrayList<String>(numClasses);                       //values of class attribute
        ArrayList<ArrayList<String>> nominals = new ArrayList<ArrayList<String>>();      //values of other nominal attributes
        while (true) {
            Object[] outputData = null;

            if (scn.hasNext()) {
                numInstant++;
                String eventStr = scn.nextLine();
                String[] event = eventStr.split(",");                      //Split data using ','
                if (event[paramCount - 1].equals("?")) {                   // Check this evet use to train or predict
                    cepEvent[paramCount - 1] = -1;                         // If it is prediction event set class value to -1
                } else {
                    String classValue = event[(paramCount - 1)];
                    if (classes.contains(classValue)) {                    // Check this class value already exists in the classes array
                        cepEvent[paramCount - 1] = classes.indexOf(classValue);   //set class value as the index of class value in the class array
                    } else {
                        if (classes.size() < numClasses) {                          //If this class value not in class array add it to class array & set the index of that value to class value
                            System.out.println("class value " + classValue);
                            classes.add(classValue);
                            cepEvent[paramCount - 1] = classes.indexOf(classValue);
                        }
                    }
                }


                int j = 0;                                                  // For count nominal attributes
                for (int i = 0; i < paramCount - 1; i++) {                  // Add event attribute values
                    if (i < paramCount - 1 - nominalOption) {               // Add Numerical attributes
                        cepEvent[i] = Double.parseDouble(event[i]);
                    } else {                                                // Add nominal attributes
                        String v = event[i];
                        try {
                            if (!nominals.get(j).contains(event[i])) {
                                nominals.get(j).add(v);
                            }
                        } catch (IndexOutOfBoundsException e) {              // Nominal attribute values initializing step
                            nominals.add(new ArrayList<String>());
                            nominals.get(j).add(v);
                        }
                        cepEvent[i] = (nominals.get(j).indexOf(event[i]));
                        j++;
                    }

                }
                outputData = streamingClassification.classify(cepEvent);      // Check output data

            } else {
                System.out.println(numInstant + " events added from CEP");
                break;
            }


            try {
                Thread.sleep(1);                                                // Event delay
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
    }
}

