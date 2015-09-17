package sameAsDiagnostics;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrintMissingSubjectsBoth {
/**
 *This file is for diagnosing the approximately 1.3 million sameAs links in curated_freebase_links
 *that are missing. We will figure out if both freebase and dbpedia are missing. The file
 *will take the output of PrintMissingSubjects{Dbpedia,Freebase}
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
    	if(fields.length!=4)
    		return;
    	
    	context.write(new Text(fields[0]+"\t"+fields[1]+"\t"+fields[2]), new Text(fields[3]));
    	
    	
		
    	
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
	     
	     
	     boolean fm=false;
	     boolean dm=false;
	     for (Text val : values) {
	    	 
	        String k=val.toString();
	        if(k.equals("dm"))
	        	dm=true;
	        else if(k.equals("fm"))
	        	fm=true;
	        
	        	
	        
	        
	        
	      }
	     	if(fm && dm)
	    	 context.write(key, new Text("fm\tdm"));
	     	else if(fm)
	     		context.write(key, new Text("fm"));
	     	else if(dm)
	     		context.write(key, new Text("dm"));
	     	else
	     		return;
	     
	     
	     
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
    Job job = new Job(conf, "sameAs diagnostics: freebase and dbpedia");
    job.setJarByClass(PrintMissingSubjectsBoth.class);
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
