package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class question4 {
    public static int estimate(SparkSession spark, JavaRDD<String> rddRatings) {
        int ans=0;
        // your code goes here

        double epsilon = 0.1;
        double delta = 0.1;
        int r = (int) Math.round(1/(epsilon*epsilon)*Math.log(1/delta));
        int bitmap = 32;
        int[] a = new Random().ints(r, 1, 1001).toArray();
        int[] b = new Random().ints(r, 0, 1001).toArray();


        // Create a RDD of songId
        int[][] hashes = rddRatings.map(line -> {
            String[] parts = line.split(",");
            int songID = Integer.parseInt(parts[1]);
            return songID;
        })
        .map(songID -> {
            //Initialize array of zeros
            int[][] vectors = new int[r][bitmap];
            for(int i=0; i<r; i++){
                for(int j=0; j<bitmap;j++){
                    vectors[i][j] = 0;
                }
            }

            for(int i=0; i < r; i++){
                int hash_value = (a[i]*songID + b[i]) % bitmap;
                vectors[i][bitmap - 1 - hash_value] = 1;
            }

            return vectors;
        })
        .reduce((s1,s2) -> {
            int[][] res = new int[r][bitmap];
            for(int i=0; i<r; i++){
                for(int j=0; j<bitmap;j++){
                    res[i][j] = s1[i][j] | s2[i][j];
                }
            }
            return s1;
        });

    	JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<int[]> vectorRDD = jsc.parallelize(Arrays.asList(hashes), 1);

        JavaRDD<Integer> trailingZeros = vectorRDD.map(vector -> {
            int res = 0;
            for(int i=bitmap-1; i>-1; i--){
                if(vector[i] == 0){
                    res = i;
                    break;
                }
            }
            return res;
        });

        Integer totalSum = trailingZeros.reduce((x,y) -> x+y);

        ans = (int) Math.round(1.3 * (Math.pow(2, totalSum/r)));

        return ans;
    }
}
