package pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;

public class SortMapper extends Mapper<LongWritable, Text, SortPair, NullWritable> {
	
	//private SortPair sp = new SortPair();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        
        
        
        
		SortPair sp = new SortPair(new Text(values[0]), Double.parseDouble(values[1].toString()));
		context.write(sp, NullWritable.get());
	}
	
}
