set default_parallel 10;
REGISTER file:/home/hadoop/lib/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

/* Load the flights data */
FLIGHTS = LOAD 's3n://cs6240bucket/data/data.csv'
            USING CSVLoader();

F1 = FOREACH FLIGHTS GENERATE (chararray) $11 AS source, (chararray) $17 AS dest;
F2 = FOREACH FLIGHTS GENERATE (chararray) $17 AS source, (chararray) $11 AS dest;
ALL_EDGES = UNION F1, F2;
DISTINCT_EDGES = DISTINCT ALL_EDGES;
EDGES = FOREACH DISTINCT_EDGES GENERATE $0 AS source, $1 AS dest;
CANON_EDGES_1 = filter EDGES by source < dest;
CANON_EDGES_2 = filter EDGES by source < dest;
TRIAD_JOIN    = join CANON_EDGES_1 by dest, CANON_EDGES_2 by source;
OPEN_EDGES    = foreach TRIAD_JOIN generate CANON_EDGES_1::source, CANON_EDGES_2::dest;
TRIANGLE_JOIN = join CANON_EDGES_1 by (source,dest), OPEN_EDGES by (CANON_EDGES_1::source, CANON_EDGES_2::dest);
TRIANGLES     = foreach TRIANGLE_JOIN generate 1 as a:int;
CONST_GROUP   = group TRIANGLES ALL parallel 1;
FINAL_COUNT   = foreach CONST_GROUP generate COUNT(TRIANGLES);
STORE FINAL_COUNT INTO 's3n://cs6240bucket/cs6240project/trianglecountingpig/output';
