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

public class EvaluatePairsCompleteness1 {
/**
 * Meant for taking freebaseDbpediaAppend and one of the f-db-*score*.txt files
 * (with a threshold pre-programmed in the mapper)
 * and producing the output:
 * <idy>[tab]<f-type>[tab]<d-type>[tab]<idx>
 * Note that all types <idy> are guaranteed to be above the pre-programmed threshold.
 * The second part of this program should deduplicate the idxs so we can serially
 * count the number of lines and determine the PC. The RR can be computed serially,
 * at least to a lower bound. 
 * 
 * To control data skew, we disregard types that cover more than 5000 sameAs links.
 * We believe this is necessary anyways in a practical setting.
 * This is similar
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  
	  
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	double threshold=0.0; //let's start with this
    	
    	//precautionary lower-case conversion
    	String line = value.toString().toLowerCase();
    	String[] fields=line.split("\t");
    	//came from the score file
    	if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("y")){
    		double score=Double.parseDouble(fields[3]);
    		if(score<=threshold)
    			return;
    		context.write(new Text(fields[1]+"\t"+fields[2]), new Text(fields[0]));
    		
    	}
    	else if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("x")){	
    		
    		//came from freebaseDbpediaAppend (note dbpedia types are all dbpedia.org/ontology)
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
    				context.write(new Text(f+"\t"+d), new Text(fields[0]));
    	}
			
    	
		
    	
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
		  String idy=new String("");
		  HashSet<String> idx=new HashSet<String>(5000);
		    int count=0;
		     for (Text val : values) {
		    	 if(count>5000)
		    		 return;
		        String k=val.toString();
		        if(k.substring(k.length()-1, k.length()).equals("y"))
		        	idy=k;
		        else if(k.substring(k.length()-1, k.length()).equals("x")){
		        	idx.add(k);
		        	count++;
		        }
		        else
		        	return;
		        
		      
		        }
		        
		     if(idy.length()==0)
		    	 return;
		     Text g=new Text(idy+"\t"+key.toString());
		      
		     for(String x:idx)
		    	  context.write(g, new Text(x));
		     
		     
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
    Job job = new Job(conf, "build pairs completeness file");
    job.setJarByClass(EvaluatePairsCompleteness1.class);
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
