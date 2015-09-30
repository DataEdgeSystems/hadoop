package census;

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

public class TSG2 {
/**
 * 
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

	 
	  
	  static String[] tokenizer={"/", ",", ":", ";", "\\(", "\\)", "\\.", 
			"\"", "_", "-", "#", "\\\\", "\\s+"};	//everything here will be replaced with space when processing.
		//as a safety measure, we convert the json object to lowercase.
	  static double idfThreshold=30000;	//change this threshold according to the dataset; see note in TSG1
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	HashSet<String> props=new HashSet<String>();
    	
    	if(line.trim().length()==0)
    		return;	//malformed line--empty; may not be malformed in future editions
    	String[] u1=line.split("\t");
    	if(u1.length==3){
    		if(Double.parseDouble(u1[2])>=idfThreshold)
    			context.write(new Text(u1[1]), new Text("IDF is present"));
    		return;
    	}
		if(!u1[1].equals("{") || !u1[u1.length-1].equals("}"))
			return;	//malformed line--braces missing
		for(int i=3; i<u1.length-1; i++){	//we'll ignore subject
			String[] attributes=u1[i].toLowerCase().split("\":\\[");
			if(attributes.length!=2)
				return;	//malformed attribute
			String prop=attributes[0];
			prop=prop.replaceAll("\"", "");
			String values=attributes[1];
			
			
			if(!values.contains("]"))
				return;	//attribute close bracket missing
			values=values.replaceAll("\\]","");
			String[] list=values.split("\", \"");
			
			
			//only tokens are relevant; the value itself is not.
				for(int j=0; j<list.length; j++){
					for(String t: tokenizer)
						list[j]=list[j].replaceAll(t, " ").trim();
					String[] tokens=list[j].split(" ");
					for(String t: tokens)
						props.add(t);
				}
			
			
			}
		
		
		
		for(String t: props)
			context.write(new Text(t), new Text(line));
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  static double jaccardThreshold=0.8;
	  static int purgeThreshold=50;	//we will tolerate up to 50 entities/dataset
	  static double logTFIDFThreshold=0.001;	//remove very improbable sample pairs
	  static String[] tokenizer={"/", ",", ":", ";", "\\(", "\\)", "\\.", 
				"\"", "_", "-", "#", "\\\\", "\\s+"};
	  
	  public static <T>int unionCardinality(Set<T> set1, Set<T> set2){
			int result=set1.size();
			for(T t: set2)
				if(!set1.contains(t))
					result++;
			return result;
		}
		
		public static <T>int intersectionCardinality(Set<T> set1, Set<T> set2){
			int result=0;
			for(T t: set1)
				if(set2.contains(t))
					result++;
			return result;
		}
		
		public static double computeJaccard(Set<String> set1, Set<String> set2){
			int union=unionCardinality(set1, set2);
			int intersection=intersectionCardinality(set1, set2);
			if(union==0.0)
				return 0.0; else
			return 1.0*intersection/union;
		}
		
		public double computeLogTFIDF(HashMap<String,Integer> tf1, HashMap<String,Integer>tf2){
			double result=0.0;
			HashMap<String, Double> logTFIDFVector1=new HashMap<String, Double>();
			HashMap<String, Double> logTFIDFVector2=new HashMap<String, Double>();
			
			
			double total=0.0;
			for(String token: tf1.keySet()){
				if(!tf2.containsKey(token))
					continue;
				double logscore=Math.log(tf1.get(token)+1)*Math.log(idf1.get(token)+1);
				logTFIDFVector1.put(token, logscore);
				total+=(logscore*logscore);
			}
			total=Math.sqrt(total);
			for(String token: logTFIDFVector1.keySet()){
				double p=logTFIDFVector1.get(token)/total;
				logTFIDFVector1.put(token, p);
			}
			
			total=0.0;
			for(String token: tf2.keySet()){
				if(!tf1.containsKey(token))
					continue;
				double logscore=Math.log(tf2.get(token)+1)*Math.log(idf2.get(token)+1);
				logTFIDFVector2.put(token, logscore);
				total+=(logscore*logscore);
			}
			total=Math.sqrt(total);
			for(String token: logTFIDFVector2.keySet()){
				double p=logTFIDFVector2.get(token)/total;
				logTFIDFVector2.put(token, p);
			}
			
			
			for(String key: logTFIDFVector1.keySet())
				result+=(logTFIDFVector1.get(key)*logTFIDFVector2.get(key));
			
			return result;
		}
	  
	  
	  public static HashMap<String, Double> buildIDF(HashSet<String> jsons){
		  HashMap<String, Double> result=new HashMap<String, Double>();
		  for(String json:jsons){
		  HashSet<String> props=new HashSet<String>();
		  String[] u1=json.split("\t");
		  
		  if(!u1[1].equals("{") || !u1[u1.length-1].equals("}"))
				return null;	//malformed line--braces missing
			for(int i=3; i<u1.length-1; i++){	//we'll ignore subject
				String[] attributes=u1[i].toLowerCase().split("\":\\[");
				if(attributes.length!=2)
					return null;	//malformed attribute
				String prop=attributes[0];
				prop=prop.replaceAll("\"", "");
				String values=attributes[1];
				
				
				if(!values.contains("]"))
					return null;	//attribute close bracket missing
				values=values.replaceAll("\\]","");
				String[] list=values.split("\", \"");
				
				
				//only tokens are relevant; the value itself is not.
					for(int j=0; j<list.length; j++){
						for(String t: tokenizer)
							list[j]=list[j].replaceAll(t, " ").trim();
						String[] tokens=list[j].split(" ");
						for(String t: tokens)
							props.add(t);
						
					}
				
				
				}
			//update IDF vector
			for(String prop: props){
				if(!result.containsKey(prop))
					result.put(prop, 0.0);
				double g=result.get(prop);
				result.put(prop, g+1.0);
			}
			
		  }
		  
		  //normalize
		  double total=jsons.size();
		  Set<String> keys=result.keySet();
		  for(String key: keys){
			  double h=result.get(key);
			  result.put(key, total/h);
		  }
		  
		  return result;
	  }


	public static HashMap<String, Integer> tokenizeJSONAndReturnTFs(String json){
		  HashMap<String, Integer> props=new HashMap<String, Integer>();
		  String[] u1=json.split("\t");
		  
		  if(!u1[1].equals("{") || !u1[u1.length-1].equals("}"))
				return null;	//malformed line--braces missing
			for(int i=3; i<u1.length-1; i++){	//we'll ignore subject
				String[] attributes=u1[i].toLowerCase().split("\":\\[");
				if(attributes.length!=2)
					return null;	//malformed attribute
				String prop=attributes[0];
				prop=prop.replaceAll("\"", "");
				String values=attributes[1];
				
				
				if(!values.contains("]"))
					return null;	//attribute close bracket missing
				values=values.replaceAll("\\]","");
				String[] list=values.split("\", \"");
				
				
				//only tokens are relevant; the value itself is not.
					for(int j=0; j<list.length; j++){
						for(String t: tokenizer)
							list[j]=list[j].replaceAll(t, " ").trim();
						String[] tokens=list[j].split(" ");
						for(String t: tokens){
							if(!props.containsKey(t))
								props.put(t,0);
							int c=props.get(t);
							props.put(t, c+1);
						}
					}
				
				
				}
		  
		  return props;
	  }
	  
		HashMap<String, Double> idf1=new HashMap<String, Double>();
		HashMap<String, Double> idf2=new HashMap<String, Double>();
	    
		public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      HashSet<String> dataset1=new HashSet<String>();	//store jsons from the org dataset
	      HashSet<String> dataset2=new HashSet<String>();	//store jsons from the dup dataset
	      boolean idfValid=false;
	      
	      
	      
	      for (Text val : values) {
	        String v=val.toString();
	    	  
	        if(v.equals("IDF is present"))
	        	idfValid=true;
	        else{
	        	String id=v.split("\t")[0];
	        	if(id.contains("dup")){
	        		if(dataset2.size()<purgeThreshold)
	        			dataset2.add(v);
	        	}
	        	else if(id.contains("org")){
	        		if(dataset1.size()<purgeThreshold)
	        			dataset1.add(v);
	        	}
	        }
	        
	    	if(dataset1.size()>=purgeThreshold && dataset2.size()>=purgeThreshold)
	    		break;
	        
	        
	      }
	      
	      
	      if(!idfValid)
	    	  return;
	      
	      if(dataset1.size()==0 || dataset2.size()==0)
	    	  return;
	     
	      
	      idf1=buildIDF(dataset1);
	      idf2=buildIDF(dataset2);
	      
	      idf1.remove(key.toString());
	      idf2.remove(key.toString());
	      
	      for(String d1: dataset1){
	    	  HashMap<String, Integer> tf1=tokenizeJSONAndReturnTFs(d1);
	    	  tf1.remove(key.toString());
	    	  for(String d2: dataset2){
	    		  HashMap<String, Integer> tf2=tokenizeJSONAndReturnTFs(d2);
	    		  tf2.remove(key.toString());
	    		  if(computeLogTFIDF(tf1, tf2)<logTFIDFThreshold) 
	    			  continue;
	    		  double jaccard=computeJaccard(tf1.keySet(), tf2.keySet());
	    		  if(jaccard>=jaccardThreshold)
	    			  context.write(new Text(d1.split("\t")[0]+"\t"+d2.split("\t")[0]), new Text(Double.toString(jaccard)));
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
    Job job = new Job(conf, "census: TSG-2");
    job.setJarByClass(TSG2.class);
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
