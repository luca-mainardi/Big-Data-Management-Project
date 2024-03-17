package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vectors;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;


public class question3 {
    static int sparseVectorSize = (int) Math.pow(2, 31);

    public static SparseVector joinSparseVector(SparseVector v1, SparseVector v2) {
                
        int i = 0; //index for v1
        int j = 0; //index for v2
        int k = 0; //index for new vector
        
        // We create new arrays for indices and values
        int indexSize = v1.indices().length + v2.indices().length;

        int[] newIndicesArray = new int[indexSize];
        double[] newValuesArray = new double[indexSize];

        // We iterate through the two vectors and add the values to the new array
        while(i < v1.indices().length && j < v2.indices().length){
            if(v1.indices()[i] < v2.indices()[j]){
                newIndicesArray[k] = v1.indices()[i];
                newValuesArray[k] = v1.values()[i];
                i++;
            } 
            else{ // We exploit the fact that two indices cannot be equal for the same user
                newIndicesArray[k] = v2.indices()[j];
                newValuesArray[k] = v2.values()[j];
                j++;
            }
            k++;
        }

        // Finish i
        while(i < v1.indices().length){
            newIndicesArray[k] = v1.indices()[i];
            newValuesArray[k] = v1.values()[i];
            i++;
            k++;
        }

        // Finish j
        while(j < v2.indices().length){
            newIndicesArray[k] = v2.indices()[j];
            newValuesArray[k] = v2.values()[j];
            j++;
            k++;
        }

        // We return the new vector
        return (SparseVector) Vectors.sparse(sparseVectorSize, newIndicesArray, newValuesArray);
    }

    // We create a function to calculate the cosine similarity between two vectors
    public static double cosineSimilarity(SparseVector v1, SparseVector v2) {

            double dotProduct = v1.dot(v2);
            double norm1 = Vectors.norm(v1, 2);
            double norm2 = Vectors.norm(v2, 2);

        return dotProduct / (double) (norm1 * norm2);
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
            .mapValues(pair -> (SparseVector) Vectors.sparse(sparseVectorSize, new int[] {pair._1 - 1}, new double[] { pair._2}))
            .reduceByKey((t1, t2) -> joinSparseVector(t1,t2));
        
        // Cartesian product to generate pairs
        JavaPairRDD<Tuple2<Integer, SparseVector>, Tuple2<Integer, SparseVector>> cartesianPairs = userTotalRatings
            .cartesian(userTotalRatings)
            .filter(pair -> pair._1._1 < pair._2._1); // To avoid duplicate pairs and self-comparisons

        // Calculate  cosine similarities
        JavaPairRDD<Integer, Tuple2<Integer, Double>> cosineSimilarities = cartesianPairs
            .mapToPair(pair -> new Tuple2<> (pair._1._1, new Tuple2<>(pair._2._1, cosineSimilarity(pair._1._2, pair._2._2))));

        // Check the requirements
        JavaPairRDD<Integer, Tuple2<Integer, Double>> upperCosineSimilarities = cosineSimilarities.filter(pair -> pair._2._2 > 0.95);

        // Return max value(s)
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
