package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.Enumeration;
import java.util.Vector;
import java.*;
import java.io.BufferedReader;
import java.io.FileReader;

public class PageRank {
	
    public enum MyCounters {
        NodeCounter, DanglingCounter, FuckedCounter
    }
    
    public static final int NumReducer = 32;
    public static final double dampingFactor = 0.85;
    
    public static void main(String[] args) throws Exception {  
    
        long nodeCount, danglingCount;
        int iter = 0;
        Vector<Double> v = new Vector<Double>();
    
        if (args.length == 3) {
            System.out.println();
            System.out.println("------------ mode: custom ------------");
            System.out.println();
        } else if (args.length == 2) {
            System.out.println();
            System.out.println("------------ mode: converge ------------");
            System.out.println();
        } else {
            System.out.println("Number of arguments is not correct.");
            System.exit(0);
        }
    
        System.out.println();
        System.out.println("************* Parsing ****************");
        System.out.println();
        Parse job1 = new Parse(args[0], "PageRank/Temp/temp");
        final long startTime = System.currentTimeMillis();
        nodeCount = job1.Parse();
        final long endTime = System.currentTimeMillis();
        System.out.println("--------------------Total execution time: " + (endTime - startTime) );
    
        System.out.println();
        System.out.println("************* Reversing ****************");
        System.out.println();
        Reverse job2 = new Reverse("PageRank/Temp/temp", "PageRank/Temp/iter" + String.valueOf(iter));
        danglingCount = job2.Reverse(nodeCount);
        
        Runtime rt = Runtime.getRuntime();
        
        if (args.length == 3) {
            int iterations = Integer.parseInt(args[2]);
            while (iter < iterations) {
                
                System.out.println();
                System.out.println("************* Calculating Dangling (" + iter + ") ****************");
                System.out.println();
                CalculateDangling job3 = new CalculateDangling("PageRank/Temp/iter" + String.valueOf(iter), "PageRank/Temp/iter" + String.valueOf(iter) + "-dangling");
                job3.CalculateDangling(nodeCount);
            
                Process pr1 = rt.exec("rm -f Temp0");
                pr1.waitFor();
                Process pr2 = rt.exec("hdfs dfs -getmerge PageRank/Temp/iter" + String.valueOf(iter) + "-dangling Temp0");
                pr2.waitFor();
                
                BufferedReader br = new BufferedReader(new FileReader("Temp0"));
                String line = br.readLine();
                br.close();
                String[] data = line.split("\t");
                double danglingSumMean = (double) Double.parseDouble(data[1]);// / (double) nodeCount;
                System.out.println(danglingSumMean);
                
                System.out.println();
                System.out.println("************* Calculating (" + iter + ") ****************");
                System.out.println();
                Calculate job4 = new Calculate("PageRank/Temp/iter" + String.valueOf(iter), "PageRank/Temp/iter" + String.valueOf(iter + 1));
                job4.Calculate(nodeCount, dampingFactor, danglingSumMean);
                
                iter++;
            }
        } else if (args.length == 2) {
            while (true) {
                
                System.out.println();
                System.out.println("************* Calculating Dangling (" + iter + ") ****************");
                System.out.println();
                CalculateDangling job3 = new CalculateDangling("PageRank/Temp/iter" + String.valueOf(iter), "PageRank/Temp/iter" + String.valueOf(iter) + "-dangling");
                job3.CalculateDangling(nodeCount);
            
                Process pr1 = rt.exec("rm -f Temp0");
                pr1.waitFor();
                Process pr2 = rt.exec("hdfs dfs -getmerge PageRank/Temp/iter" + String.valueOf(iter) + "-dangling Temp0");
                pr2.waitFor();
                
                BufferedReader br = new BufferedReader(new FileReader("Temp0"));
                String line = br.readLine();
                br.close();
                String[] data = line.split("\t");
                double danglingSumMean = (double) Double.parseDouble(data[1]);// / (double) nodeCount;
                System.out.println(danglingSumMean);
                
                System.out.println();
                System.out.println("************* Calculating (" + iter + ") ****************");
                System.out.println();
                Calculate job4 = new Calculate("PageRank/Temp/iter" + String.valueOf(iter), "PageRank/Temp/iter" + String.valueOf(iter + 1));
                job4.Calculate(nodeCount, dampingFactor, danglingSumMean);
                
                //TODO: check error and break the loop
                System.out.println();
                System.out.println("************* Calculating Error (" + iter + ") ****************");
                System.out.println();
                CalculateError job5 = new CalculateError("PageRank/Temp/iter" + String.valueOf(iter), "PageRank/Temp/iter" + String.valueOf(iter + 1), "PageRank/Temp/iter" + String.valueOf(iter) + "-" + String.valueOf(iter + 1) + "-error");
                job5.CalculateError();
                
                System.out.println();
                System.out.println("************* Calculating Error Sum (" + iter + ") ****************");
                System.out.println();
                SumError job6 = new SumError("PageRank/Temp/iter" + String.valueOf(iter) + "-" + String.valueOf(iter + 1) + "-error", "PageRank/Temp/iter" + String.valueOf(iter) + "-" + String.valueOf(iter + 1) + "-error-sum");
                job6.SumError();
                
                Process pr3 = rt.exec("rm -f Temp1");
                pr3.waitFor();
                Process pr4 = rt.exec("hdfs dfs -getmerge PageRank/Temp/iter" + String.valueOf(iter) + "-" + String.valueOf(iter + 1) + "-error-sum Temp1");
                pr4.waitFor();
                
                BufferedReader br1 = new BufferedReader(new FileReader("Temp1"));
                String line1 = br1.readLine();
                br1.close();
                String[] data1 = line1.split("\t");
                double errorSum = (double) Double.parseDouble(data1[1]);
                System.out.println("!!!!!!!!!!!!Error: " + errorSum + "!!!!!!!!!!!!");
                v.addElement(errorSum);
                
                iter++;
                if (errorSum < 0.001) break;
            }
        }
    
        System.out.println();
        System.out.println("************* Sorting ****************");
        System.out.println();
        Sort job5 = new Sort("PageRank/Temp/iter" + String.valueOf(iter), "PageRank/Output");
        job5.Sort();
        
        Process pr1 = rt.exec("rm -f Temp0 Temp1");
        pr1.waitFor();
        
        if (args.length == 2) {
            Enumeration<Double> vEnum = v.elements();
            while (vEnum.hasMoreElements()) {
                System.out.print(vEnum.nextElement() + " ");
            }
            System.out.println();
        }
    
        System.out.println("Number of nodes: " + nodeCount);
        System.out.println("Number of dangling nodes: " + danglingCount);
        System.exit(0);
    }  
}
