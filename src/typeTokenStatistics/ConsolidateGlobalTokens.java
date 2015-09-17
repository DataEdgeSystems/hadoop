package typeTokenStatistics;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ConsolidateGlobalTokens {
/**
 * This file is only for extracting global token sets. Property distinctions will be ignored.
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  /**
	   * Meant for taking a global token file as input and producing a condensed version
	   * with a type occurring in one line only.
	   */
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	String[] fields=line.split("\t");
    	if(fields.length!=3)
    		return;
    	int k=Integer.parseInt(fields[2]);
    	if(k<=5)
    		return;
    	fields[1]=fields[1].replaceAll("'", "");
    	fields[1]=fields[1].replaceAll("<", "");
    	fields[1]=fields[1].replaceAll(">", "");
    	try{
    		Long.parseLong(fields[1]);
    		return;
    	}catch(NumberFormatException e){
    		
    	}
		
    	context.write(new Text(fields[0]), new Text(fields[1]+"\t"+fields[2]));
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  
	    public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     HashSet<String> tokenCounts=new HashSet<String>(31000);
	     int count=0;
	     int capacity=30000;
	     for (Text val : values) {
	    	 if(count>capacity)
	    		 break;
	        String k=val.toString();
	        tokenCounts.add(k);
	        count++;
	        
	      }
	      String result=new String(""); 
	      for(String k: tokenCounts)
	    	  result+=(k+"\t");
	      if(result.length()<=1)
	    	  return;
	      result=result.substring(0,result.length()-1);
	      context.write(key, new Text(result));
	    }
	  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new String[2];
    otherArgs[0]=args[0];
    otherArgs[1]=args[1];
     /* new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }*/
    Job job = new Job(conf, "consolidating global type statistics");
    job.setJarByClass(ConsolidateGlobalTokens.class);
    job.setMapperClass(TokenizerMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
