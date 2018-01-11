package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class Sort {
	
    private String inPath;
    private String outPath;
    
	public Sort(String inPath, String outPath) {
        this.inPath = inPath;
        this.outPath = outPath;
    }
	
	public void Sort() throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Sort");
		job.setJarByClass(Sort.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(SortMapper.class);
		job.setPartitionerClass(SortPartitioner.class);
		job.setReducerClass(SortReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(SortPair.class);
		job.setMapOutputValueClass(NullWritable.class);
		//job.setOutputKeyClass(xxx.class);
		//job.setOutputValueClass(xxx.class);
		
		// set the number of reducer
		job.setNumReduceTasks(1);
		
		// add input/output path
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(outPath)))
            fs.delete(new Path(outPath), true);
		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.waitForCompletion(true);
	}
}
