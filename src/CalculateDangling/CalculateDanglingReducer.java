package pagerank;

import java.io.IOException;
import java.lang.StringBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

public class CalculateDanglingReducer extends Reducer<Text,Text,Text,Text> {
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
        Configuration conf = context.getConfiguration();
        long N = Long.parseLong(conf.get("N"));
        
        double sum = 0;
        for (Text text: values) {
            sum += (double) Double.parseDouble(text.toString()) / (double) N;
        }
        context.write(key, new Text(Double.toString(sum)));
        
	}
}
