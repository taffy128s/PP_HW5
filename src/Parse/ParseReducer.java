package pagerank;

import java.io.IOException;
import java.lang.StringBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParseReducer extends Reducer<Text,Text,Text,Text> {
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean isAlive = false;
        boolean head = true;
        StringBuffer str = new StringBuffer();
        
        for (Text text: values) {
            if (text.toString().equals("")) {
                context.getCounter(PageRank.MyCounters.FuckedCounter).increment(1);
            }
            if (text.toString().equals("!*!")) {
                isAlive = true;
                continue;
            }
            if (head) {
                head = false;
                str.append(text.toString());
            } else {
                str.append("\t" + text.toString());
            }
        }
        
        if (isAlive)
            context.write(key, new Text(str.toString()));
	}
}
