package pagerank;

import java.io.IOException;
import java.lang.StringBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

public class CalculateReducer extends Reducer<Text,Text,Text,Text> {
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
        StringBuffer str = new StringBuffer();
        Configuration conf = context.getConfiguration();
        double dampingFactor = Double.parseDouble(conf.get("dampingFactor"));
        double sum = 0;
        double temp = 0;
        
        for (Text text: values) {
            if (text.toString().startsWith("|")) {
                str.append("\t" + text.toString().substring(1));
            } else if (text.toString().startsWith("*")) {
                temp += Double.parseDouble(text.toString().substring(1));
            } else {
                sum += Double.parseDouble(text.toString());
            }
        }
        sum += temp * dampingFactor;
        context.write(key, new Text(String.valueOf(sum) + str.toString()));
        
	}
}
