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

public class DbpediaAppendObjectYago {
/**
 * Meant for taking the Dbpedia withTypes and the curated_yago_links and
 * returning a file of the format:
 * <idz>[tab]<yago-entity>[tab]"dbpedia-instance"[tab]<dbpedia-json-object>
 * 
 *Several points are important:
 *1. The double quotes around dbpedia-instance are meta-characters and do not actually occur
 *2. Recall that the original json objects were of format <subject>[tab]<json-object>. We
 *will NOT be printing out the subject part, to conserve space.
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
    	if(fields.length!=3 && !fields[1].equals("{"))
    		return;
    	//came from links file
    	if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("z")){
    		context.write(new Text(fields[2]), new Text(fields[0]+"\t"+fields[1]));
    	}else{//came from withTypes file: send the whole line
    		
    			context.write(new Text(fields[0]), new Text(line));
    	}
    	
		
    	
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
	     String res=new String("dbpedia-instance\t");
	     boolean flag=true;
	     HashSet<String> yagoEntities=new HashSet<String>();
	     for (Text val : values) {
	    	 
	        String k=val.toString();
	        String[] fields=k.split("\t");
	        if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("z"))
	        	yagoEntities.add(k);
	        else if(flag){
	        	flag=false;
	        	for(int i=1; i<fields.length-1; i++)
	        		res+=(fields[i]+"\t");
	        	res=res+fields[fields.length-1];
	        	
	        }
	        
	        
	      }
	     if(yagoEntities.size()==0||res.equals("dbpedia-instance\t"))
	    	 return;
	     
	     for(String f: yagoEntities)
	    	 context.write(new Text(f), new Text(res));
	     
	     
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
    Job job = new Job(conf, "append dbpedia resources and yago entities");
    job.setJarByClass(DbpediaAppendObjectYago.class);
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
