package org.example;

import java.util.BitSet;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

class question5 {

    public static List<Tuple4<Integer, Integer, Integer, Integer>> solution(SparkSession spark,
            JavaRDD<String> rddRatings) {

        System.out.println("Question 5 start: ");

        List<Tuple4<Integer, Integer, Integer, Integer>> ans = null;
        // your code goes here

        // Create a PairRDD of (userID, songID) -> rating
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> userRatings = rddRatings.mapToPair(line -> {
            String[] parts = line.split(",");
            int userID = Integer.parseInt(parts[0]);
            int songID = Integer.parseInt(parts[1]);
            int rating = (parts.length == 3) ? Integer.parseInt(parts[2]) : -1; // -1 indicates no rating
            return new Tuple2<>(new Tuple2<>(userID, songID), rating);
        });

        // Filter out lines with no ratings
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> ratedUserSong = userRatings
                .filter(pair -> pair._2 != -1);

        // map to pair to get (userId, songId)
        JavaPairRDD<Integer, BitSet> userSongPairs = ratedUserSong
                .mapToPair(pair -> {
                    BitSet bitSet = new BitSet();
                    bitSet.set(pair._1._2);

                    return new Tuple2<Integer, BitSet>(pair._1._1, bitSet);
                });

        // reduce by key to get BitSets representing lists of songs for each user
        JavaPairRDD<Integer, BitSet> userSongs = userSongPairs.reduceByKey((songs1, songs2) -> {
            BitSet mergedSongs = new BitSet();
            mergedSongs.or(songs1);
            mergedSongs.or(songs2);
            return mergedSongs;
        });

        JavaPairRDD<Integer, BitSet> validCountUserSongs = userSongs.filter(pair -> pair._2().cardinality() < 8000);

        // Compute cartesian product to get all possible pairs of users and their
        // distinct songs
        JavaPairRDD<Tuple2<Integer, Integer>, BitSet> firstCartesian = validCountUserSongs
                .cartesian(validCountUserSongs).filter(pair -> pair._1()._1() < pair._2()._1()).mapToPair(pair -> {
                    int userId1 = pair._1()._1();
                    int userId2 = pair._2()._1();
                    BitSet set1 = pair._1()._2();
                    BitSet set2 = pair._2()._2();

                    BitSet mergedSet = new BitSet();
                    mergedSet.or(set1);
                    mergedSet.or(set2);

                    return new Tuple2<>(new Tuple2<>(userId1, userId2), mergedSet);
                });

        // Filter out pairs with more than 8000 songs
        JavaPairRDD<Tuple2<Integer, Integer>, BitSet> validCountPairSongs = firstCartesian
                .filter(pair -> pair._2().cardinality() < 8000);

        // Compute cartesian product to get all possible triplets of users and their
        // distinct songs
        JavaPairRDD<Tuple3<Integer, Integer, Integer>, BitSet> secondCartesian = validCountPairSongs
                .cartesian(validCountUserSongs).filter(pair -> pair._1()._1()._2() < pair._2()._1())
                .mapToPair(pair -> {
                    int userId1 = pair._1()._1()._1();
                    int userId2 = pair._1()._1()._2();
                    int userId3 = pair._2()._1();
                    BitSet set1 = pair._1()._2();
                    BitSet set2 = pair._2()._2();

                    BitSet mergedSet = new BitSet();
                    mergedSet.or(set1);
                    mergedSet.or(set2);

                    return new Tuple2<>(new Tuple3<>(userId1, userId2, userId3), mergedSet);
                });

        // Filter out triplets with more than 8000 songs
        JavaPairRDD<Tuple3<Integer, Integer, Integer>, BitSet> validCountTripletSongs = secondCartesian
                .filter(pair -> pair._2().cardinality() < 8000);

        // Emit the result tuple
        JavaRDD<Tuple4<Integer, Integer, Integer, Integer>> tripletsDistinctSongsCount = validCountTripletSongs
                .map(tuple -> {
                    int userId1 = tuple._1()._1();
                    int userId2 = tuple._1()._2();
                    int userId3 = tuple._1()._3();
                    int distinctCount = tuple._2().cardinality();
                    return new Tuple4<>(userId1, userId2, userId3, distinctCount);
                });

        ans = tripletsDistinctSongsCount.collect();

        return ans;

    }

}
