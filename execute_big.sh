iter=$1

INPUT_FILE=/user/ta/PageRank/Input/input-1G
OUTPUT_FILE=PageRank/Output
JAR=PageRank.jar

hdfs dfs -rm -r PageRank
hadoop jar $JAR pagerank.PageRank $INPUT_FILE $OUTPUT_FILE $iter
hdfs dfs -getmerge $OUTPUT_FILE pagerank-1G.txt

INPUT_FILE=/user/ta/PageRank/Input/input-10G
hdfs dfs -rm -r PageRank
hadoop jar $JAR pagerank.PageRank $INPUT_FILE $OUTPUT_FILE $iter
hdfs dfs -getmerge $OUTPUT_FILE pagerank-10G.txt

INPUT_FILE=/user/ta/PageRank/Input/input-50G
hdfs dfs -rm -r PageRank
hadoop jar $JAR pagerank.PageRank $INPUT_FILE $OUTPUT_FILE $iter
hdfs dfs -getmerge $OUTPUT_FILE pagerank-50G.txt

hw5-fancydiff 1G 10 pagerank-1G.txt
hw5-fancydiff 10G 10 pagerank-10G.txt
hw5-fancydiff 50G 10 pagerank-50G.txt
