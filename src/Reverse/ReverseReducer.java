package pagerank;

import java.io.IOException;
import java.lang.StringBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

public class ReverseReducer extends Reducer<Text,Text,Text,Text> {
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
        Configuration conf = context.getConfiguration();
        StringBuffer str = new StringBuffer();
        boolean isDangling = true;
        long v = Long.parseLong(conf.get("nodeCount"));
        double val = (double) 1 / (double) v;
        str.append(Double.toString(val));
        
        for (Text text: values) {
            if (text.toString().equals(""))
                continue;
            str.append("\t" + text);
            isDangling = false;
        }
        if (isDangling)
            context.getCounter(PageRank.MyCounters.DanglingCounter).increment(1);
        context.write(key, new Text(str.toString()));
        
	}
}
