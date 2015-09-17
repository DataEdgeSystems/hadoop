package typeGoldStandard;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FreebaseTypes {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
/*
 * This file was written to extract freebase types. The result is a two-field tab
 * separated file of the form <freebase-subject> <freebase-type>
 */
	  

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	try{
    	String line = value.toString();
    	
    	if(!line.substring(0,1).equals("#"))
    	{ 
        	
	  		String[] nodes=line.toLowerCase().split("\t");
	  		if(nodes.length==4 && nodes[1].equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")){
	  			
  				context.write(new Text(nodes[0]), new Text(nodes[2]));
  			
  		
  			}	
    	}
    	}catch(Exception e){e.printStackTrace();}
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
   // private Text result = new Text();
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
     
    	
	     
	     for (Text val : values) {
	    	 context.write(key, val);
	     }
    
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
    Job job = new Job(conf, "freebase types");
    job.setJarByClass(FreebaseTypes.class);
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
