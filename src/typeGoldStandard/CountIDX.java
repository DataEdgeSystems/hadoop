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

public class CountIDX {
/**
 * Meant for taking freebaseDbpediaAppend and producing a count file in the style:
 * <fr-type>[tab]<db-type>[tab]<count>
 * where count refers to the number of sameAs links (or IDXs) covered by that type. The counts may obviously 'overlap', but the
 * goal of this file is to diagnose (potential) data skew in the GenerateIDXY file.
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
    	//came from the score file
    	if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("x")){
    		
    	HashSet<String> freebaseTypes=new HashSet<String>();
    		HashSet<String> dbpediaTypes=new HashSet<String>();
    		for(int i=1; i<fields.length; i++){
    			if(fields[i].contains("rdf.freebase.com"))
    				freebaseTypes.add(fields[i]);
    			else if(fields[i].contains("dbpedia.org/ontology"))
    				dbpediaTypes.add(fields[i]);
    		}
    		//String g=fields[0].replaceAll("/m.", "/m/");
    		for(String f: freebaseTypes)
    			for(String d: dbpediaTypes)
    		context.write(new Text(f+"\t"+d), new Text("1"));
    	}
			
    	
		
    	
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
		  long sum=0;
		    
		     for (Text val : values) {
		    	 
		        sum++;
		      
		        }
		        
		      String res=Long.toString(sum);
		      context.write(key, new Text(res));
		     
		     
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
    Job job = new Job(conf, "count idxs in freebase");
    job.setJarByClass(CountIDX.class);
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
