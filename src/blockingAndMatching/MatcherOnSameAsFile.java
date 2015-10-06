package blockingAndMatching;

import java.io.FileInputStream;
import java.io.FileReader;
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

public class MatcherOnSameAsFile {
	
	
	
/**
 * Because of scaling problems with the other Matcher files, and limited experimental
 * time left, we built this file. Basically, it runs the model file on the
 * freebaseDbpediaSameAs, printing out all pairs for which the match threshold
 * is exceeded in the reducer. This is a controlled experiment testing the maximum
 * recall of the matcher. Make sure to run a similar experiment on non-sameAs
 * links to test the estimated precision.
 * 
 * Ran experiments both with 0.5 and 0.0 (for matcherThresholds) Note that the
 * semantics are now greater than, rather than greater than equal to.
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

	  
	  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	
    	context.write(value, new Text(""));
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  
	  public static Classifier classifier=null;
	  public boolean classifierOn=false;
	//  static int purgeThreshold=10;	//we will tolerate up to 10 entities/dataset
	//  static int reducerThreshold=100; //reducer will not allow more than this many entities/bkv to be processed
	  static double matcherThreshold=0.0;
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
	  
	//we've made this foolproof and tested it.
			private static boolean isAlphabeticOnly(String key){
				boolean result=true;
				String lc=key.toLowerCase();
				String uc=key.toUpperCase();
				for(int i=0; i<key.length(); i++)
					if(lc.charAt(i)==(uc.charAt(i)))
						return false;
				return result;
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
		
		/*
		 * We're ignoring the integer part. 
		 */
		public static ArrayList<HashSet<String>> prepForAlphaJaccard(ArrayList<HashMap<String,Integer>> list){
			ArrayList<HashSet<String>> preppedList=new ArrayList<HashSet<String>>();
			for(HashMap<String,Integer> map:list){
				Set<String> keys=map.keySet();
				HashSet<String> tmp=new HashSet<String>();
				for(String key:keys)
					if(isAlphabeticOnly(key))
						tmp.add(key);
				preppedList.add(tmp);
			}
			return preppedList;
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
		
		public static String extractSubjectFromInstance(String instance){
			String subject=instance.split("\t")[0];
			String[] fields=subject.split("\":\\[\"");
			if(!fields[0].substring(1, fields[0].length()).equals("subject")){
				//System.out.println("Error! Expected subject but got "+fields[0].substring(1, fields[0].length()));
				return null;
			}
			else return fields[1].substring(0, fields[1].length()-2);
				
		}
		
		public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      
	      
	      Path[] cacheFiles = context.getLocalCacheFiles();
	     // context.write(new Text(Integer.toString(cacheFiles.length)), new Text(cacheFiles[0].toString()));
	      
	      if(!classifierOn)
	    	  {
	    	  
	    	  ObjectInputStream in=new ObjectInputStream(new FileInputStream(cacheFiles[0].toString()));
	  	
	  		try {
	  			if(classifier==null)
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
	      
	      
	     String[] fields=key.toString().split("\t\\{\t|\t\\}");
	     
	      String d1=fields[1];
	    	  
	    	  ArrayList<HashSet<String>> prepped1=prepForAlphaJaccard(parseJSONIntoStringFeatures(d1.split("\t")));
	    	 
	    	  String d2=fields[3];
	    		  
		    	  
		    	  ArrayList<Double> instance=
							(extractJaccardFeatures(
									prepForAlphaJaccard(
										parseJSONIntoStringFeatures(d2.split("\t"))), prepped1
									));
					instance.add(0.0);
					FastVector attributes=createAttributes(instance.size());
					Instances test=new Instances("instances", attributes, 1);
					test.add(new Instance(1.0,convertToArray(instance)));
					test.setClassIndex(test.numAttributes() - 1);
					try {
						double prob=classifier.distributionForInstance(test.instance(0))[0];
						if(prob>matcherThreshold)
							context.write(new Text(extractSubjectFromInstance(d1)
									+"\t"+extractSubjectFromInstance(d2)),
									new Text(Double.toString(prob)));
					} 
					
					catch (Exception e) {
						
						context.write(new Text("Error in loop"), new Text());
						return;
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
    Job job = new Job(conf, "freebase-dbpedia: Matcher on sameAs");
    DistributedCache.addCacheFile(new URI(args[2]),job.getConfiguration());
  //  DistributedCache.addCacheFile(new URI(args[3]),job.getConfiguration());
    job.setJarByClass(MatcherOnSameAsFile.class);
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
