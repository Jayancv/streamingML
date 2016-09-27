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
            File f = new File("result.csv");
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            scn = new Scanner(br);

        } catch (Exception e) {
            logger.info(e.toString());
        }
        int learnType = 0;
        int paramCount = 5;
        int batchSize = 1000;
        int numClasses = 3;
        int maxinstance = 100000;

        StreamingClassification streamingClassification = new StreamingClassification(maxinstance, batchSize, numClasses, paramCount);

        new Thread(streamingClassification).start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        double[] cepEvent = new double[paramCount];
        List<String> classes = new ArrayList<String>();
        while (true) {
            Object[] outputData = null;

            if (scn.hasNext()) {
                String eventStr = scn.nextLine();
                String[] event = eventStr.split(",");
                int k = numClasses - 1;
                String cla = event[(paramCount - 1)];
                if (classes.contains(cla)) {
                    cepEvent[paramCount - 1] = classes.indexOf(cla);
                } else {
                    classes.add(cla);
                    cepEvent[paramCount - 1] = classes.indexOf(cla);
                }

                for (int i = 0; i < paramCount - 1; i++) {
                    cepEvent[i] = Double.parseDouble(event[i]);

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

