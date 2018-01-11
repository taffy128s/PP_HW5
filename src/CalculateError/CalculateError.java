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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.util.HashSet;
import java.util.Set;

public class CalculateError {
	
    private String inPath1;
    private String inPath2;
    private String outPath;
    
	public CalculateError(String inPath1, String inPath2, String outPath) {
        this.inPath1 = inPath1;
        this.inPath2 = inPath2;
        this.outPath = outPath;
  	}
	
	public void CalculateError() throws Exception {
		Configuration conf = new Configuration();
        
		Job job = Job.getInstance(conf, "CalculateError");
		job.setJarByClass(CalculateError.class);	
		
		//job.setMapperClass(CalculateErrorMapper.class);
        MultipleInputs.addInputPath(job, new Path(inPath1), TextInputFormat.class, CalculateErrorMapper1.class);
        MultipleInputs.addInputPath(job, new Path(inPath2), TextInputFormat.class, CalculateErrorMapper2.class);
		job.setReducerClass(CalculateErrorReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(PageRank.NumReducer);
		
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(outPath)))
            fs.delete(new Path(outPath), true);
		//FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		
		job.waitForCompletion(true);
    }
}
