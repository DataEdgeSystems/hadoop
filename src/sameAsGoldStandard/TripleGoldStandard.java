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

public class TripleGoldStandard {
/**
 * An ambitious three-way undertaking that will take freebaseDbpediaSameAsAppend and yagoDbpediaSameAsAppend
 * and return a file of the format:
 * <idx>[tab]<idz>[tab]"yago-instance"[tab]<yago-json-objects>[tab]
 * "freebase-instance"[tab]<freebase-json-object)>[tab]"dbpedia-instance"[tab]<dbpedia-json-object)>
 * Please see the header in DbpediaAppendObject for the caveats on the json objects and
 * the double quotes meta-characters.
 * 
 * Don't get confused! This file will contain the curly braces. Note that incomplete
 * links will not get published; every line will necessarily be of the format above.
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  
	  
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString().toLowerCase();
    	String[] fields=line.split("\t\\{\t|\t\\}");
    	
    	
    	context.write(new Text(fields[3]), new Text(fields[0]+"\t"+fields[1]));
    	
			
    	
		
    	
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
		//  String res=new String("freebase-instance\t");
		    // boolean flag=true;
		     HashSet<String> yagoEntities=new HashSet<String>();
		    HashSet<String> freebaseEntities=new HashSet<String>();
		     for (Text val : values) {
		    	 
		        String k=val.toString();
		        String[] fields=k.split("\t");
		        if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("x"))
		        	freebaseEntities.add(k);
		        else if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("z"))
		        	yagoEntities.add(k);
		        
		        
		      }
		     if(yagoEntities.size()==0||freebaseEntities.size()==0)
		    	 return;
		     
		     String dbpediaObject="{\t"+key.toString()+"\t}";
		     for(String yago: yagoEntities){
		    	 String[] yagoFields=yago.split("\t");
		    	 String idz=yagoFields[0];
		    	 String yagoObject=new String("");
		    	 yagoObject+="{\t";
		    	 for(int i=2; i<yagoFields.length; i++)
		    		 yagoObject+=(yagoFields[i]+"\t");
		    	 yagoObject+="}";
		    	 for(String freebase: freebaseEntities){
		    		 String[] freebaseFields=freebase.split("\t");
			    	 String idx=freebaseFields[0];
			    	 String freebaseObject=new String("");
			    	 freebaseObject+="{\t";
			    	 for(int i=2; i<freebaseFields.length; i++)
			    		 freebaseObject+=(freebaseFields[i]+"\t");
			    	 freebaseObject+="}";
			    	 String result=idx+"\t"+idz+"\t"+"yago-instance\t"+yagoObject+"\tfreebase-instance\t"+freebaseObject;
			    	 context.write(new Text(result), new Text("dbpedia-instance\t"+dbpediaObject));
		    	 }
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
    Job job = new Job(conf, "join sameAs files for freebase-dbpedia-yago");
    job.setJarByClass(TripleGoldStandard.class);
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
