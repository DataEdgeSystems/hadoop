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

public class DbpediaAppendTypes {
/**
 * Meant for taking the Dbpedia curated_instance-types and the curated_freebase_links and
 * returning a file of the format:
 * <idx>[tab]<freebase-entity1>...<dbpedia-type1>[tab]<dbpedia-type2>...
 * 
 * More than one type occurs in a line if a dbpedia resource has multiple types. We only
 * consider dbpedia.org/ontology types. 
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  
	  
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString().toLowerCase();
    	String[] fields=line.split("\t");
    	//wayward line
    	if(fields.length!=3)
    		return;
    	//came from links file
    	if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("x")){
    		context.write(new Text(fields[2]), new Text(fields[0]+"\t"+fields[1]));
    	}else{//came from types file
    		if(fields[2].contains("dbpedia.org/ontology"))
    			context.write(new Text(fields[0]), new Text(fields[2]));
    	}
    	
		
    	
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
	     HashSet<String> types=new HashSet<String>();
	     HashSet<String> freebaseEntities=new HashSet<String>();
	     for (Text val : values) {
	    	 
	        String k=val.toString();
	        if(k.contains("rdf.freebase.com"))
	        	freebaseEntities.add(k);
	        else if(k.contains("dbpedia.org/ontology"))
	        	types.add(k);
	        
	        
	      }
	     if(freebaseEntities.size()==0||types.size()==0)
	    	 return;
	     String result=new String("");
	     for(String g: types)
	    	 result+=(g+"\t");
	     if(result.length()<=1)
	    	 return;
	     else
	    	 result=result.substring(0, result.length()-1);
	     
	     for(String f: freebaseEntities)
	    	 context.write(new Text(f), new Text(result));
	     
	     
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
    Job job = new Job(conf, "append types of dbpedia resources and freebase entities");
    job.setJarByClass(DbpediaAppendTypes.class);
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
