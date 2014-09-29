  SELECT artist, COUNT(*) AS cnt
    FROM ${table}
GROUP BY artist
ORDER BY cnt DESC
   LIMIT 2;
