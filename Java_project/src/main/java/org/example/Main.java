package org.example;

import org.apache.commons.math3.util.Precision;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getRootLogger().setLevel(Level.OFF);

        String master = "local[*]";
        SparkConf conf = new SparkConf()
                .setAppName(Main.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession
                .builder()
                .appName("2AMD15")
                .getOrCreate();
        String path = "Java_project/dataset/plays.txt";

        // Question 0: Load the data as an RDD and as a DataFrame
        JavaRDD<String> rddRatings = null;
        Dataset<Row> df1 = null;
        // Your code for question 0 goes here

        /*
         * sc.textFile(path) is used to read the text file at the specified path into an
         * RDD of Strings, where each String represents a line in the text file.
         */
        rddRatings = sc.textFile(path);

        // Define the schema
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("user_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("song_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("rating", DataTypes.IntegerType, false)
        });

        // Read the file into a DataFrame using the defined schema
        df1 = spark.read().schema(schema).option("delimiter", ",").csv(path);

        // After initializing the RDD and DataFrame
        long rddCount = rddRatings.count();
        long dfCount = df1.count();

        System.out.println("Number of elements in RDD: " + rddCount);
        System.out.println("Number of elements in DataFrame: " + dfCount);

        // Print a preview of the data frame
        df1.show(5);
        // Question 0 END

        long startTime = System.currentTimeMillis();
        long endTime;
        long ans1 = question1.solution(spark, df1);
        System.out.println(">> Q1: " + ans1);
        endTime = System.currentTimeMillis();
        System.out.println("Q1 time: " + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        Tuple2<Integer, Float> ans2 = question2.solution(spark, rddRatings);
        System.out.println(">> Q2: " + ans2._1 + ", " + ans2._2);
        endTime = System.currentTimeMillis();
        System.out.println("Q2 time: " + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        for (Tuple3<Integer, Integer, Double> t : question3.solution(spark, rddRatings)) {
            System.out.println(">> Q3: <" + t._1() + ", " + t._2() + ", " + Precision.round(t._3(), 2) + ">");
        }
        endTime = System.currentTimeMillis();
        System.out.println("Q3 time: " + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        System.out.println(">> Q4: " + question4.estimate(spark, rddRatings));
        endTime = System.currentTimeMillis();
        System.out.println("Q4 time: " + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        for (Tuple4<Integer, Integer, Integer, Integer> t : question5.solution(spark, rddRatings)) {
            System.out.println(">> Q5: <" + t._1() + "," + t._2() + "," + t._3() + "," + t._4() + ">");
        }
        endTime = System.currentTimeMillis();
        System.out.println("Q5 time: " + (endTime - startTime) + "ms");
    }
}