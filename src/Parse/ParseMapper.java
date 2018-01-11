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

public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
		Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		Matcher titleMatcher = titlePattern.matcher(value.toString());
        
        titleMatcher.find();
        String title = unescapeXML(titleMatcher.group(1));
        context.getCounter(PageRank.MyCounters.NodeCounter).increment(1);
        
        
        Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
		Matcher linkMatcher = linkPattern.matcher(value.toString());
        
        context.write(new Text(title), new Text("!*!"));
        while (linkMatcher.find()) {
            String target = unescapeXML(capitalizeFirstLetter(linkMatcher.group(1)));
            context.write(new Text(target), new Text(title));
        }
	}
	
	private String unescapeXML(String input) {

		return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'");

    }

    private String capitalizeFirstLetter(String input){

    	char firstChar = input.charAt(0);

        if ( firstChar >= 'a' && firstChar <='z'){
            if ( input.length() == 1 ){
                return input.toUpperCase();
            }
            else
                return input.substring(0, 1).toUpperCase() + input.substring(1);
        }
        else 
        	return input;
    }
}
