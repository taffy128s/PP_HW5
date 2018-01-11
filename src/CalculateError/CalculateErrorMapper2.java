package pagerank;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.net.URI; 
import java.io.*;

public class CalculateErrorMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String[] values = value.toString().split("\t");
        
        context.write(new Text(values[0]), new Text("@" + values[1]));
        
        /*String[] values = value.toString().split("\t");
        
        Configuration conf = context.getConfiguration();
        double dampingFactor = Double.parseDouble(conf.get("dampingFactor"));
        double danglingSumMean = Double.parseDouble(conf.get("danglingSumMean"));
        long N = Long.parseLong(conf.get("N"));
        
        double divider = (double) (values.length - 2);
        double rank = (double) Double.parseDouble(values[1]);
        double toAdd = (rank / divider);// * dampingFactor;
        for (int i = 2; i < values.length; i++) {
            if (values[i].equals(""))
                context.getCounter(PageRank.MyCounters.FuckedCounter).increment(1);
            context.write(new Text(values[i]), new Text("*" + Double.toString(toAdd)));
            context.write(new Text(values[0]), new Text("|" + values[i]));
        }
        toAdd = danglingSumMean * dampingFactor;
        context.write(new Text(values[0]), new Text(Double.toString(toAdd)));
        toAdd = ((double) 1 - dampingFactor) / (double) N;
        context.write(new Text(values[0]), new Text(Double.toString(toAdd)));*/
        
	}
}
