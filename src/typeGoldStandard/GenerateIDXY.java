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
@Deprecated
public class GenerateIDXY {
/**
 * Meant for taking freebaseDbpediaAppend and one of the f-db-*score*.txt files
 * and producing the output:
 * <idy>[tab]<idx1>[tab]...<idxn>[tab]<score>
 * Intuitively, we are determining which type-pairs (idy) are 'covering' which instance pairs,
 * and also what our jaccard file indicates (note that we re-generated our files to print
 * zero-scores also. This guarantees that every type-pair has an id).
 * This is akin to forming a perfect set of blocks. I believe it can be analyzed serially,
 * but we'll only know once the program terminates.
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
    	if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("y")){
    		
    		context.write(new Text(fields[1]+"\t"+fields[2]), new Text(fields[0]+"\t"+fields[3]));
    		
    	}
    	else{	//came from freebaseDbpediaAppend (note dbpedia types are all dbpedia.org/ontology
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
	     
		  HashSet<String> scoreField=new HashSet<String>();
		  HashSet<String> idx=new HashSet<String>();
		    
		     for (Text val : values) {
		    	 
		        String k=val.toString();
		        String[] fields=k.split("\t");
		        if(fields.length==2)
		        	scoreField.add(k);
		        else if(fields.length==1)
		        	idx.add(k);
		        else return;
		      
		        }
		        
		      String[] scoreFieldString=new String[2];  
		      if(scoreField.size()!=1 || idx.size()==0)
		    	  return;
		      else
		    	  for(String p: scoreField)
		    		  scoreFieldString=p.split("\t");
		      
		      String id=new String("");
		      
		      for(String x:idx)
		    	  id+=(x+"\t");
		      id+=scoreFieldString[1];
		      context.write(new Text(scoreFieldString[0]), new Text(id));
		     
		     
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
    Job job = new Job(conf, "build idy list(idx) score file");
    job.setJarByClass(GenerateIDXY.class);
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
