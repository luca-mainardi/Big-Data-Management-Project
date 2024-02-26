package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class question2 {
    public static Tuple2<Integer, Float> solution(SparkSession spark, JavaRDD<String> rddRatings) {

        // Create a PairRDD of (userID, songID) -> rating
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> userRatings = rddRatings.mapToPair(line -> {
            String[] parts = line.split(",");
            int userID = Integer.parseInt(parts[0]);
            int songID = Integer.parseInt(parts[1]);
            int rating = (parts.length == 3) ? Integer.parseInt(parts[2]) : -1; // -1 indicates no rating
            return new Tuple2<>(new Tuple2<>(userID, songID), rating);
        });

        // Filter out lines with no ratings
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> ratedUserSongs = userRatings.filter(pair -> pair._2 != -1);

        // Reduce by key to get total rating and count for each user
        // userID -> (totalRating, ratingCount)
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> userTotalRatings = ratedUserSongs
                .mapToPair(pair -> new Tuple2<>(pair._1._1, new Tuple2<>(pair._2, 1)))
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2));

        // Filter users with at least 100 ratings
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> usersWith100RatingsOrMore = userTotalRatings
                .filter(pair -> pair._2._2 >= 100);

        // Calculate the average rating for each user
        JavaPairRDD<Integer, Float> userAverageRatings = usersWith100RatingsOrMore
                .mapValues(pair -> (float) pair._1 / pair._2);

        // Find the user with the minimum average rating among users with at least 100
        // ratings
        Tuple2<Integer, Float> grumpiestUser = userAverageRatings.reduce((t1, t2) -> t1._2 < t2._2 ? t1 : t2);

        return grumpiestUser;

    }
}
