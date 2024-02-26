SELECT user_id, AVG(rating) as average_rating
FROM ratings_table
GROUP BY user_id
HAVING COUNT(DISTINCT song_id) > 100


SELECT COUNT(DISTINCT user_id) 
FROM (  SELECT user_id, AVG(rating) AS avg_rating 
        FROM df1 
        GROUP BY user_id 
        HAVING COUNT(rating) >= 100 
        AND AVG(rating) < 2) 
AS subquery

SELECT COUNT(*)
FROM(
    SELECT user_id
    FROM your_table_name
    WHERE rating IS NOT NULL
    GROUP BY user_id
    HAVING COUNT(*) >= 100 AND AVG(rating) < 2);

