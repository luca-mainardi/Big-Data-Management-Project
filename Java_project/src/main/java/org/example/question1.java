package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question1 {
    public static long solution(SparkSession spark, Dataset<Row> df1) {

        // Create a temporary view of the DataFrame
        df1.createOrReplaceTempView("df1");

        // long ans = spark.sql(
        // "SELECT COUNT(DISTINCT user_id) FROM (SELECT user_id, AVG(rating) AS
        // avg_rating FROM df1 GROUP BY user_id HAVING COUNT(*) >= 100 AND AVG(rating) <
        // 2) AS subquery")
        // .first().getLong(0);
        // 4628ms

        long ans = spark.sql(
                "SELECT COUNT(*) AS num_users\n" + //
                        "FROM (\n" + //
                        " SELECT user_id\n" + //
                        " FROM (\n" + //
                        " SELECT user_id, AVG(rating) AS avg_rating\n" + //
                        " FROM df1\n" + //
                        " WHERE rating IS NOT NULL\n" + //
                        " GROUP BY user_id\n" + //
                        " HAVING COUNT(rating) >= 100 AND AVG(rating) < 2\n" + //
                        " ) AS subquery\n" + //
                        ") AS final_query;\n" + //
                        "")
                .first().getLong(0);
        // 3993ms

        // long ans = spark.sql(
        // "SELECT COUNT(*) FROM(SELECT user_id FROM df1 WHERE rating IS NOT NULL GROUP
        // BY user_id HAVING COUNT(*) >= 100 AND AVG(rating) < 2)")
        // .first().getLong(0);
        // 4132ms

        return ans;
    }
}
