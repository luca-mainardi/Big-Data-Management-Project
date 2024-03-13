package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vectors;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;


public class question3 {

    public static SparseVector joinSparseVector(SparseVector v1, SparseVector v2) {

        int size = v1.size() > v2.size() ? v1.size() : v2.size();
        List<Integer> indices = new ArrayList<>();
        List<Double> values = new ArrayList<>();


        SparseVector shorter; SparseVector longer;

        if(v1.indices().length < v2.indices().length){
            shorter = v1;
            longer = v2;
        }else{
            shorter = v2;
            longer = v1;
        }

        //i index for shorter, j index for longer
        int i = 0; int j = 0;
        while(j<longer.indices().length) {
            int longer_index = longer.indices()[j];
            double longer_value = longer.values()[j];

            if(i < shorter.indices().length){
                int shorter_index = shorter.indices()[i];
                double shorter_value = shorter.values()[i];

                if(shorter_index < longer_index){
                    indices.add(shorter_index);
                    values.add(shorter_value);
                    i++;
                }
                else{
                    indices.add(longer_index);
                    values.add(longer_value);
                    j++;
                }
            }
            else {
                indices.add(longer_index);
                values.add(longer_value);
                j++;
            }
        }

        //create the arrays
        int[] indices_array = new int[indices.size()];
        double[] values_array = new double[values.size()];

        for(int k = 0; k < indices.size(); k++) {
            indices_array[k] = indices.get(k);
        }

        for(int k = 0; k < values.size(); k++) {
            values_array[k] = values.get(k);
        }

        return (SparseVector) Vectors.sparse(size, indices_array, values_array);
    }

    public static double cosineSimilarity(SparseVector v1, SparseVector v2) {
            //check the sizes first in order to do the dot product
            int size = v1.size() < v2.size() ? v1.size() : v2.size();

            Vector w1 = Vectors.sparse(size, v1.indices(), v1.values());
            Vector w2 = Vectors.sparse(size, v2.indices(), v2.values());

            double dotProduct = w1.dot(w2);
            double norm1 = Vectors.norm(w1, 2);
            double norm2 = Vectors.norm(w2, 2);

        return dotProduct / (norm1 * norm2);
    }

    public static List<Tuple3<Integer, Integer, Double>> solution(SparkSession spark, JavaRDD<String> rddRatings) {
        List<Tuple3<Integer, Integer, Double>> ans = new ArrayList<>();
        
        // Create a PairRDD of userID -> (songID, rating)
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> userRatings = rddRatings.mapToPair(line -> {
            String[] parts = line.split(",");
            int userID = Integer.parseInt(parts[0]);
            int songID = Integer.parseInt(parts[1]);
            int rating = (parts.length == 3) ? Integer.parseInt(parts[2]) : -1; // -1 indicates no rating
            return new Tuple2<>(userID, new Tuple2<>(songID, rating));
        });

        // Filter out lines with no ratings
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> ratedUserSongs = userRatings.filter(pair -> pair._2._2 != -1);

        // Reduce by key to get Vector of ratings 
        // userID -> Vector
        JavaPairRDD<Integer, SparseVector> userTotalRatings = ratedUserSongs
            .mapValues(pair -> (SparseVector) Vectors.sparse(pair._1 + 1, new int[] {pair._1}, new double[] {pair._2}))
            .reduceByKey((t1, t2) -> joinSparseVector(t1,t2));
        
        // Cartesian product to generate pairs
        JavaPairRDD<Tuple2<Integer, SparseVector>, Tuple2<Integer, SparseVector>> cartesianPairs = userTotalRatings
            .cartesian(userTotalRatings)
            .filter(pair -> pair._1._1 < pair._2._1); // To avoid duplicate pairs and self-comparisons

        //Calculate  cosine similarities
        JavaPairRDD<Integer, Tuple2<Integer, Double>> cosineSimilarities = cartesianPairs
            .mapToPair(pair -> new Tuple2<> (pair._1._1, new Tuple2<>(pair._2._1, cosineSimilarity(pair._1._2, pair._2._2))));
    

        //Check the requirements
        JavaPairRDD<Integer, Tuple2<Integer, Double>> upperCosineSimilarities = cosineSimilarities.filter(pair -> pair._2._2 > 0.95);

        //return max value
        JavaRDD<Tuple3<Integer, Integer, Double>> results = upperCosineSimilarities.reduceByKey((t1, t2) -> {
            if (t1._2 > t2._2){
                return t1;
            }else if(t1._2 == t2._2){
                return t1._1 < t2._1 ? t1 : t2;
            }else {
                return t2;
            }

        }).map(pair -> new Tuple3<> (pair._1(), pair._2()._1(), pair._2()._2()));

        ans = results.collect(); 
        
        return ans;
    }

}
