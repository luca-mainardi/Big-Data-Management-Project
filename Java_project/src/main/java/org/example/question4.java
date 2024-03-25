package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Random;

public class question4 {
    public static int estimate(SparkSession spark, JavaRDD<String> rddRatings) {
        int ans;

        float epsilon = (float) 0.1;
        float delta = (float) 0.1;
        int r = (int) Math.ceil(Math.log(1 / delta) / Math.pow(epsilon, 2));
        int bitmap = 32;
        Random random = new Random();
        int[] a = random.ints(r, 1, 1001).toArray();
        int[] b = random.ints(r, 0, 1001).toArray();


        // Create a RDD of songId
        long[] hashes = rddRatings.map(line -> {
            String[] parts = line.split(",");
            int songID = Integer.parseInt(parts[1]);
            return songID;
        })
        .map(songID -> {
            //Initialize array of zeros
            long[] vectors = new long[r];
            for(int i=0; i<r; i++){
                vectors[i] = 0;
            }
            //set to 1 the index given by the hash value
            for(int i=0; i < r; i++){
                int hash_value = (a[i]*songID + b[i]) % bitmap;
                vectors[i] |= (long) Math.pow(2, hash_value);  
            }
            return vectors;
        })
        .reduce((s1,s2) -> {
            //join using the or operator
            for(int i=0; i<r; i++){
                s1[i] |= s2[i];
            }
            return s1;
        });
        
        //sum the positions of the left most zeroes
        int totalSum = 0;
        for(int j=0; j<r; j++){
            int left_most = bitmap-1;
            for(int i=0; i<bitmap; i++){
                if(((hashes[j]>>i)&1) == 0){
                    left_most = i;
                    break;
                }
                
            }
            totalSum += left_most;
        }

        //give estimates
        ans = (int) Math.round(1.2928 * Math.pow(2, 1.0*totalSum/r));

        return ans;

    }
}
