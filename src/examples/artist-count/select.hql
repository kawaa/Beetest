  SELECT artist, COUNT(*) AS cnt
    FROM stream
GROUP BY artist
ORDER BY cnt DESC
   LIMIT 2;
