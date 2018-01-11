package pagerank;

import java.io.IOException;
import java.lang.StringBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

public class SumErrorReducer extends Reducer<Text,Text,Text,Text> {
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
        double sum = 0;
        
        for (Text text: values) {
            sum += Double.parseDouble(text.toString());
        }
        
        context.write(key, new Text(String.valueOf(sum)));
        
	}
}
