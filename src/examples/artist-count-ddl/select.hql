  SELECT artist, COUNT(*) AS cnt
    FROM streamed_songs
GROUP BY artist
ORDER BY cnt DESC
   LIMIT 2;
