package sameAsGoldStandard;

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

public class PruneCuratedLinksFile {
/**
 * Meant for taking the consolidatedTypesSubjects and the curated_freebase_links and
 * returning a file that only contains those links, such that both subjects are
 * guaranteed to have non-missing json objects.
 *
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  
	  
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	//the toLowercase is a precautionary measure.
    	String line = value.toString().toLowerCase();
    	String[] fields=line.split("\t");
    	//wayward line
    	if(!fields[0].substring(fields[0].length()-1, fields[0].length()).equals("x"))
    		return;
    	//came from links file
    	if(fields.length==3){
    		context.write(new Text(line), new Text("from-links"));
    	}else{//came from withTypes file: send the whole line
    		for(int i=3; i<fields.length; i++)
    			if(fields[i].equals("dm")||fields[i].equals("fm")){
    				context.write(new Text(fields[0]+"\t"+fields[1]+"\t"+fields[2]),
    						new Text("present"));
    				return;
    			}
    	}
    	
		
    	
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
	     
	     
	     for (Text val : values) {
	    	 
	        String k=val.toString();
	       if(k.equals("present"))
	    	   return;
	        
	        
	      }
	    String[] fields=key.toString().split("\t");
	    context.write(new Text(fields[0]), new Text(fields[1]+"\t"+fields[2]));
	     
	     
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
    Job job = new Job(conf, "prune curated_freebase_links based on missing subjects");
    job.setJarByClass(PruneCuratedLinksFile.class);
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
