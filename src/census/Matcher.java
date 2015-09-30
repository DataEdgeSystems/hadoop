package census;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class Matcher {
	
	
	
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
	  static double idfThreshold=50000;	//change this threshold according to the dataset; see note in TSG1
	  
	  
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
	  
	  
	  public static Classifier classifier;
	  public boolean classifierOn=false;
	  static int purgeThreshold=10;	//we will tolerate up to 50 entities/dataset
	  static double matcherThreshold=0.5;
	  static String[] tokenizer={"/", ",", ":", ";", "\\(", "\\)", "\\.", 
				"\"", "_", "-", "#", "\\\\", "\\s+"};
	  
	  
	  public static double[] convertToArray(ArrayList<Double> instance){
			double[] k=new double[instance.size()];
			for(int i=0; i<instance.size(); i++)
				k[i]=instance.get(i);
			return k;
		}
	  
	  public static ArrayList<Double> extractJaccardFeatures(ArrayList<HashSet<String>> preppedDB, ArrayList<HashSet<String>> preppedFB){
			ArrayList<Double> features=new ArrayList<Double>();
			for(int i=0; i<preppedDB.size(); i++)
				for(int j=0; j<preppedFB.size(); j++){
					features.add(computeJaccard(preppedDB.get(i), preppedFB.get(j)));
				}
			return features;
		}
	  
	  public static ArrayList<HashSet<String>> prepForNonAlphaJaccard(ArrayList<HashMap<String,Integer>> list){
			ArrayList<HashSet<String>> preppedList=new ArrayList<HashSet<String>>();
			for(HashMap<String,Integer> map:list){
				Set<String> keys=map.keySet();
				HashSet<String> tmp=new HashSet<String>();
				for(String key:keys)
					
						tmp.add(key);
				preppedList.add(tmp);
			}
			return preppedList;
		}
	  
	  public static ArrayList<HashMap<String, Integer>> parseJSONIntoStringFeatures(String[] json){
			ArrayList<HashMap<String, Integer>> result=new ArrayList<HashMap<String, Integer>>(3);
			result.add(new HashMap<String,Integer>());
			result.add(new HashMap<String,Integer>());
			result.add(new HashMap<String,Integer>());
			
			for(String propValues: json){
				String[] attributes=propValues.split("\":\\[");
				if(attributes.length!=2)
					{
					System.out.println("Incorrect parsing");
					System.out.println(propValues);
					continue;
					}
				//first, let's deal with P tokens
				String prop=attributes[0];
				prop=prop.replaceAll("\"", "");
				for(String t: tokenizer)
					prop=prop.replaceAll(t, " ").trim();
				String[] propTokens=prop.split(" ");
				HashSet<String> propTokensSet=new HashSet<String>();
				for(String propToken: propTokens)
					propTokensSet.add(propToken);
				for(String propToken: propTokensSet)
				{
					if(!result.get(0).containsKey(propToken))
						result.get(0).put(propToken, 0);
					int g=result.get(0).get(propToken);
					result.get(0).put(propToken, g+1);
				}
				
				//now let's deal with L and O tokens
				String values=attributes[1];
				
				
				
				values=values.replaceAll("\\]","");
				String[] list=values.split("\", \"");
				for(int j=0; j<list.length; j++){
					//is it a literal?
					boolean literal=false;
					if(list[j].length()>1&&list[j].substring(0, 1).equals("\"")&&
							list[j].substring(list[j].length()-1, list[j].length()).equals("\""))
						literal=true;
					
					for(String t: tokenizer)
						list[j]=list[j].replaceAll(t, " ").trim();
					
					String[] tokens=list[j].split(" ");
					HashSet<String> tokensSet=new HashSet<String>();
					for(String token: tokens)
						tokensSet.add(token);
					if(literal){
						for(String token: tokensSet)
						{
							if(!result.get(1).containsKey(token))
								result.get(1).put(token, 0);
							int g=result.get(1).get(token);
							result.get(1).put(token, g+1);
						}
					}
					else{
						for(String token: tokensSet)
						{
							if(!result.get(2).containsKey(token))
								result.get(2).put(token, 0);
							int g=result.get(2).get(token);
							result.get(2).put(token, g+1);
						}
					}
				}
			}
			//finally, we'll deal with A tokens. Note that this has not been added yet.
			HashMap<String,Integer> tmp=new HashMap<String,Integer>();
			for(int i=0; i<result.size(); i++){
				HashMap<String,Integer> k=result.get(i);
				for(String key: k.keySet()){
					if(!tmp.containsKey(key))
						tmp.put(key, 0);
					int num=tmp.get(key);
					tmp.put(key, num+k.get(key));
				}
			}
			result.add(tmp);
			return result;
		}
	  
	  
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
				return -1.0; else
			return 1.0*intersection/union;
		}
		
		private static FastVector createAttributes(int numAttributes){
			FastVector att=new FastVector();
			for(int i=1; i<numAttributes; i++)
				att.addElement(new Attribute("feat"+i));
			att.addElement(new Attribute("classAttribute"));
			return att;
		}
		
		public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      HashSet<String> dataset1=new HashSet<String>();	//store jsons from the org dataset
	      HashSet<String> dataset2=new HashSet<String>();	//store jsons from the dup dataset
	      boolean idfValid=false;
	      
	      Path[] cacheFiles = context.getLocalCacheFiles();
	     // context.write(new Text(Integer.toString(cacheFiles.length)), new Text(cacheFiles[0].toString()));
	      
	      if(!classifierOn)
	    	  {ObjectInputStream in=new ObjectInputStream(new FileInputStream(cacheFiles[0].toString()));
	  	
	  		try {
				classifier=(Classifier) in.readObject();
				classifierOn=true;
			} catch (ClassNotFoundException e1) {
				context.write(new Text("classifier error"), new Text());
				in.close();
		  		return;

			}
	  		in.close();
	    	  }
	  		
	      if(classifier==null){
	    	 context.write(new Text("null"), new Text("null"));
	    	  return;
	      }
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
	     
	      for(String d1: dataset1){
	    	  String[] f1=d1.split("\t");
	    	  String[] c1=new String[f1.length-4];
	    	  for(int i=3; i<f1.length-1; i++)
	    		  c1[i-3]=f1[i];
	    	  ArrayList<HashSet<String>> prepped1=prepForNonAlphaJaccard(parseJSONIntoStringFeatures(c1));
	    	  for(String d2: dataset2){
	    		  String[] f2=d2.split("\t");
		    	  String[] c2=new String[f2.length-4];
		    	  for(int i=3; i<f2.length-1; i++)
		    		  c2[i-3]=f2[i];
		    	  
		    	  ArrayList<Double> instance=
							(extractJaccardFeatures(
									prepForNonAlphaJaccard(
										parseJSONIntoStringFeatures(c2)), prepped1
									));
					instance.add(0.0);
					FastVector attributes=createAttributes(instance.size());
					Instances test=new Instances("instances", attributes, 1);
					test.add(new Instance(1.0,convertToArray(instance)));
					test.setClassIndex(test.numAttributes() - 1);
					try {
						double prob=classifier.distributionForInstance(test.instance(0))[0];
						if(prob>=matcherThreshold)
							context.write(new Text(d1.split("\t")[0]+"\t"+d2.split("\t")[0]), new Text(Double.toString(prob)));
					} 
					
					catch (Exception e) {
						
						context.write(new Text("Error in loop"), new Text());
						return;
					}
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
    Job job = new Job(conf, "census: Matcher");
    DistributedCache.addCacheFile(new URI(args[2]),job.getConfiguration());
    job.setJarByClass(Matcher.class);
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
