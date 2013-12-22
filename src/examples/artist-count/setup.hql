DROP TABLE IF EXISTS streamed_songs;

CREATE TABLE streamed_songs(artist STRING, song STRING, user STRING, ts TIMESTAMP)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'artist-count/input.tsv' INTO TABLE streamed_songs;