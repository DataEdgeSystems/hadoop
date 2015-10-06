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

public class YagoAppendObject {
/**
 * Meant for taking the output of DbpediaAppendObjectYago and the YAGO JSON files and
 * returning a file of the format:
 * <idz>[tab]"yago-instance"[tab]<yago-json-object)>[tab]"dbpedia-instance"[tab]<dbpedia-json-object)>
 * Please see the header in DbpediaAppendObjectYago for the caveats on the json objects and
 * the double quotes meta-characters.
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  
	  
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString().toLowerCase();
    	String[] fields=line.split("\t");
    	//came from dbpediaSameAsAppendYago
    	if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("z")){
    		
    		context.write(new Text(fields[1]), new Text(line));
    		
    	}
    	else{//from yago json file
    		
    		context.write(new Text(fields[0]), new Text(line));
    	}
			
    	
		
    	
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
		  String res=new String("yago-instance\t");
		     boolean flag=true;
		     HashSet<String> dbpediaEntities=new HashSet<String>();
		     for (Text val : values) {
		    	 
		        String k=val.toString();
		        String[] fields=k.split("\t");
		        if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("z"))
		        	dbpediaEntities.add(k);
		        else if(flag){
		        	flag=false;
		        	for(int i=1; i<fields.length-1; i++)
		        		res+=(fields[i]+"\t");
		        	res=res+fields[fields.length-1];
		        	
		        }
		        
		        
		      }
		     if(dbpediaEntities.size()==0||res.equals("yago-instance\t"))
		    	 return;
		     
		     for(String d: dbpediaEntities){
		    	 String[] fields=d.split("\t");
		    	 String q=fields[0]+"\t"+res;
		    	 String g=new String("");
		    	 for(int i=2; i<fields.length-1; i++)
		        		g+=(fields[i]+"\t");
		        	g=g+fields[fields.length-1];
		    	 context.write(new Text(q), new Text(g));
		    	 
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
    Job job = new Job(conf, "append yago json objects to complete sameAs ground-truth");
    job.setJarByClass(YagoAppendObject.class);
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
