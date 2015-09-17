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

public class FreebaseAppendTypes {
/**
 * Meant for taking the Freebase types files and the DBpediaAppend file and
 * returning a file of the format:
 * <idx>[tab]<freebase-type(s)>[tab]<dbpedia-type(s)>
 * We assume only dbpedia.org/ontology types for dbpedia
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  
	  
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString().toLowerCase();
    	String[] fields=line.split("\t");
    	//came from dbpediaAppend
    	if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("x")){
    		String g=new String("");
    		for(int i=2; i<fields.length; i++)
    			g+=(fields[i]+"\t");
    		g=g.substring(0, g.length()-1);
    		context.write(new Text(fields[1]), new Text(fields[0]+"\t"+g));
    		
    	}
    	else{
    		String g=fields[0].replaceAll("/m.", "/m/");
    		context.write(new Text(g), new Text(fields[1]));
    	}
			
    	
		
    	
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
	     HashSet<String> elements=new HashSet<String>();
	     HashSet<String> freebasetypes=new HashSet<String>();
	     for (Text val : values) {
	    	 
	        String line=val.toString();
	        String[] fields=line.split("\t");
	    	//came from dbpediaAppend
	    	if(fields[0].substring(fields[0].length()-1, fields[0].length()).equals("x"))
	        	elements.add(line);
	    	else
	        
	        	freebasetypes.add(line);
	        
	        
	      }
	     if(freebasetypes.size()==0||elements.size()==0)
	    	 return;
	     
	     String result=new String("");
	     for(String f: freebasetypes)
	    	 result+=(f+"\t");
	     
	     for(String e: elements){
	    	 String[] fields=e.split("\t");
	    	 String q=result+"";
	    	 for(int i=1; i<fields.length; i++)
	    		 q+=(fields[i]+"\t");
	    	 q=q.substring(0,q.length()-1);
	    	 context.write(new Text(fields[0]), new Text(q));
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
    Job job = new Job(conf, "generate freebase-dbpedia appended pairs");
    job.setJarByClass(FreebaseAppendTypes.class);
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
