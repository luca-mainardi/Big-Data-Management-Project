package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sound.midi.SysexMessage;
import javax.swing.plaf.basic.BasicInternalFrameTitlePane.SystemMenuBar;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class question4 {
    public static int estimate(SparkSession spark, JavaRDD<String> rddRatings) {
        int ans;
        // your code goes here

        float epsilon = (float) 0.1;
        float delta = (float) 0.1;
        int r = (int) Math.ceil(Math.log(1 / delta) / Math.pow(epsilon, 2));
        int bitmap = 31;
        Random random = new Random();
        int[] a = random.ints(r, 1, 1001).toArray();
        int[] b = random.ints(r, 0, 1001).toArray();


        // Create a RDD of songId
        int[] hashes = rddRatings.map(line -> {
            String[] parts = line.split(",");
            int songID = Integer.parseInt(parts[1]);
            return songID;
        })
        .map(songID -> {
            //Initialize array of zeros
            int[] vectors = new int[r];
            for(int i=0; i<r; i++){
                vectors[i] = 0;
            }

            for(int i=0; i < r; i++){
                int hash_value = (a[i]*songID + b[i]) % bitmap;
                vectors[i] += (int) Math.pow(2, hash_value);  
                // System.out.println("Hash value: "+hash_value+";  Vector : "+String.format("%32s", Integer.toBinaryString(vectors[i])).replace(' ', '0'));
            }
            return vectors;
        })
        .reduce((s1,s2) -> {
            for(int i=0; i<r; i++){
                // System.out.println("Vector 1: " + String.format("%32s",Integer.toBinaryString(s1[i])).replace(' ', '0'));
                // System.out.println("Vector 2: " + String.format("%32s",Integer.toBinaryString(s2[i])).replace(' ', '0'));
                s1[i] += s2[i];
                // System.out.println("Total: " + String.format("%32s",Integer.toBinaryString(s1[i])).replace(' ', '0'));
            }
            return s1;
        });

        int totalSum = 0;
        for(int j=0; j<r; j++){
            int left_most = bitmap-1;
            // System.out.println("Vector: " + String.format("%32s",Integer.toBinaryString(hashes[j])).replace(' ', '0'));
            for(int i=0; i<bitmap; i++){
                if(((hashes[j]>>i)&1) == 0){
                    left_most = i;
                    break;
                }
                
            }
            // System.out.println("left most: "+left_most);
            totalSum += left_most;
        }

        // for(int i=0; i<r; i++){
        //     System.out.println(String.format("%32s",Integer.toBinaryString(hashes[i])).replace(' ', '0'));
        // }

        // JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    
        // JavaRDD<Integer> vectorRDD = jsc.parallelize(Arrays.stream(hashes).boxed().collect(Collectors.toList()));

        // JavaRDD<Integer> trailingZeros = vectorRDD.map(vector -> {
            // int res = bitmap-1;
            // System.out.println(Integer.toBinaryString(vector));
            // for(int i=0; i<bitmap; i++){
            //     if(((vector>>i)&1) == 0){
            //         res = i;
            //         break;
            //     }
            // }
        //     System.out.println("Leftmost zero:" + res);
        //     return res;
        //     });

        // Integer totalSum = trailingZeros.reduce((x,y) -> x+y);

        // System.out.println("total sum: "+ totalSum);
        // System.out.println("r: "+ r);


        // System.out.println("Result: " + 1.3 * Math.pow(2, totalSum/r));

        // System.out.println(1.0*totalSum/r);

        ans = (int) Math.round(1.2928 * Math.pow(2, 1.0*totalSum/r));

        // jsc.close();

        return ans;

    }
}
