package typeTokenStatistics;

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

@Deprecated
public class DbpediaTypeTokenStatistics {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

	  private final static IntWritable one = new IntWritable(1);
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	HashMap<String, HashSet<String>> props=new HashMap<String, HashSet<String>>();
    	boolean typeTrue=false;
    	if(line.trim().length()==0)
    		return;	//malformed line--empty; may not be malformed in future editions
    	String[] u1=line.split("\t");
    	if(u1.length<=3)
    		return;	//malformed line--incorrect tab-delimited fields
		if(!u1[1].equals("{") || !u1[u1.length-1].equals("}"))
			return;	//malformed line--braces missing
		for(int i=2; i<u1.length-1; i++){
			String[] attributes=u1[i].toLowerCase().split("\":\\[");
			if(attributes.length!=2)
				return;	//malformed attribute
			String prop=attributes[0];
			prop=prop.replaceAll("\"", "");
			String values=attributes[1];
			if(props.containsKey(prop))
				return;	//attribute occurs twice
			props.put(prop, new HashSet<String>());
			
			if(!values.contains("]"))
				return;	//attribute close bracket missing
			values=values.replaceAll("\\]","");
			String[] list=values.split("\", \"");
			
			//treat type values more conservatively; do not tokenize
			if(prop.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")){
				typeTrue=true;
				for(String l: list)
					props.get(prop).add(l.replaceAll("\"", ""));
			}
				
			
			else
			
			//only tokens are relevant; the value itself is not.
				for(int j=0; j<list.length; j++){
					for(String t: ExtractGlobalTokenStatistics.TokenizerMapper.tokenizer)
						list[j]=list[j].replaceAll(t, " ").trim();
					String[] tokens=list[j].split(" ");
					for(String t: tokens)
						props.get(prop).add(t);
				}
				
				
				
			
				
			
			}
		HashSet<String> globalTokenSet=new HashSet<String>();
		if(typeTrue){
			for(String type: props.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")){
				for(String p: props.keySet())
					if(p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
						continue;
					else
						for(String token: props.get(p)){
							context.write(new Text(type+"\t"+p+"\t"+token), one);
							globalTokenSet.add(token);
						}
			}
		}
		//also record type-dependent but property-independent occurrence of tokens
		for(String t: globalTokenSet)
			for(String type: props.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
				context.write(new Text(type+"\t"+"global"+"\t"+t), one);
		
    	
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
    Job job = new Job(conf, "counting type statistics: dbpediaWithTypes");
    job.setJarByClass(DbpediaTypeTokenStatistics.class);
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
