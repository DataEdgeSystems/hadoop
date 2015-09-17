package printMalformedLines;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MalformedLinesStatistics {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
/**
 * Very similar to PrintMalformedLines, except we do not print the offending line
 * itself, to save space. I want to get a better idea of the faults we're dealing with.
 */
	  private final static IntWritable one = new IntWritable(1);
	  
	 
		
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	context.write(new Text("total"), one);
    	HashMap<String, HashSet<String>> props=new HashMap<String, HashSet<String>>();
    	boolean typeTrue=false;
    	boolean typedborg=false;
    	if(line.trim().length()==0){
    		context.write(new Text("empty"),one);
    		return;}
    	String[] u1=line.split("\t");
    	if(u1.length<=3){
    		context.write(new Text("lessthanthree"),one);
    		return;}
		if(!u1[1].equals("{") || !u1[u1.length-1].equals("}")){
			context.write(new Text("bracesmissing"),one);
			return;
		}
		for(int i=2; i<u1.length-1; i++){
			String[] attributes=u1[i].toLowerCase().split("\":\\[");
			if(attributes.length!=2){
				context.write(new Text("malformedproperty"),one);
				return;
			}
			String prop=attributes[0];
			prop=prop.replaceAll("\"", "");
			String values=attributes[1];
			if(props.containsKey(prop)){
				context.write(new Text("propertyrepetition"),one);
				return;
			}
			props.put(prop, new HashSet<String>());
			
			if(!values.contains("]")){
				context.write(new Text("malformedsquarebracket"),one);
				return;
			}
			values=values.replaceAll("\\]","");
			
			
			if(prop.contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")){
				typeTrue=true;
				if(values.contains("dbpedia.org/ontology"))
					typedborg=true;
			}
		
			}
		
		if(!typeTrue){
			context.write(new Text("typemissing"),one);
			//return;
		}
		if(!typedborg)
			context.write(new Text("dborg-type missing"), one);
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
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
    Job job = new Job(conf, "counting file statistics");
    job.setJarByClass(MalformedLinesStatistics.class);
    job.setMapperClass(TokenizerMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
