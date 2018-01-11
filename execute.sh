input=$1
iter=$2

INPUT_FILE=/user/ta/PageRank/Input/input-$input
OUTPUT_FILE=PageRank/Output
JAR=PageRank.jar

hdfs dfs -rm -r PageRank
hadoop jar $JAR pagerank.PageRank $INPUT_FILE $OUTPUT_FILE $iter
hdfs dfs -getmerge $OUTPUT_FILE pagerank-$input.txt
