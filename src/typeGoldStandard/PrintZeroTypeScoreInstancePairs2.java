package typeGoldStandard;

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

public class PrintZeroTypeScoreInstancePairs2 {
/**
 * Meant for taking the output of PrintZero...1 and pruned_curated_links
 * and producing the output:
 * <idy>[tab]<f-type>[tab]<d-type>[tab]<idx>[tab]<f-instance>[tab]<d-instance>
 * 
 * 
 * 
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  
	  
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	
    	
    	//precautionary lower-case conversion
    	String line = value.toString().toLowerCase();
    	String[] fields=line.split("\t");
    	if(fields.length!=4&&fields.length!=3)
    		return;
    	//came from the score file
    	if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("y")){
    		
    		context.write(new Text(fields[3]), new Text(line));
    		
    	}
    	else if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("x"))	
    		
    		//came from links file
    		
    				context.write(new Text(fields[0]), new Text(fields[1]+"\t"+fields[2]));
    	
			
    	
		
    	
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
		  String instance=new String("");
		  HashSet<String> idx=new HashSet<String>(5000);
		    
		     for (Text val : values) {
		    	 
		        String k=val.toString();
		        String[] fields=k.split("\t");
		        if(fields.length==2)
		        	instance=k;
		        else 
		        	idx.add(k);
		        	
		        
		      
		        }
		        
		     if(idx.size()==0 || instance.equals(""))
		    	 return;
		     
		      
		     for(String x:idx)
		    	  context.write(new Text(x), new Text(instance));
		     
		     
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
    Job job = new Job(conf, "build 0-type-score instance pairs file - pt2");
    job.setJarByClass(PrintZeroTypeScoreInstancePairs2.class);
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
