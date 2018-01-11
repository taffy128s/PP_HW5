package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import java.util.HashSet;
import java.util.Set;

public class Parse {
	
    private String inPath;
    private String outPath;
    
    public static Set<String> allPages = new HashSet<String>();
    
	public Parse(String inPath, String outPath) {
        this.inPath = inPath;
        this.outPath = outPath;
  	}
	
	public long Parse() throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Parse");
		job.setJarByClass(Parse.class);	
		
		job.setMapperClass(ParseMapper.class);
		job.setReducerClass(ParseReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(PageRank.NumReducer);
		
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(outPath)))
            fs.delete(new Path(outPath), true);
		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		
		job.waitForCompletion(true);
        return job.getCounters().findCounter(PageRank.MyCounters.NodeCounter).getValue();
        
    }
}
