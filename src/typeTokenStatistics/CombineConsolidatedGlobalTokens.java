package typeTokenStatistics;

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

public class CombineConsolidatedGlobalTokens {
/**
 * Meant for taking the consolidated files and combining them to remove inter-type-redundancies.
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  
	  
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	String[] fields=line.split("\t");
    	
		
    	context.write(new Text(fields[0]), new Text(line));
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  private static HashMap<String, String> buildDataStructure(String[] tokens){
			HashMap<String, String> result=new HashMap<String, String>(30000);
			for(int i=1; i<tokens.length; i+=2){
				result.put(tokens[i], tokens[i+1]);
			}
			
			return result;
		}
		
		
		private static HashMap<String,String> combine(HashMap<String, String> a, HashMap<String, String> b){
			HashMap<String, String> result=new HashMap<String, String>();
			for(String k: a.keySet()){
				
				if(b.containsKey(k)){
					long num=Long.parseLong(a.get(k))+Long.parseLong(b.get(k));
					result.put(k, Long.toString(num));
				}
				else result.put(k, a.get(k));
			}
			
			for(String k: b.keySet()){
				if(result.keySet().size()>30000)
					break;
				if(!a.containsKey(k))
					result.put(k, b.get(k));
				
			}
			return result;
		}
		
		private static HashMap<String,String> combine(ArrayList<String> list){
			if(list.size()<=1)
				return buildDataStructure(list.get(0).split("\t"));
			
			HashMap<String,String> result=combine(buildDataStructure(list.get(0).split("\t")),buildDataStructure(list.get(1).split("\t")));
			for(int i=2; i<list.size(); i++){
				result=combine(result,buildDataStructure(list.get(i).split("\t")));
			}
			
			
			return result;
		}
		
		private static String convert(HashMap<String, String> a){
			String result="";
			for(String k: a.keySet()){
				
				result+=(k+"\t"+a.get(k)+"\t");
			}
			
			return result.substring(0,result.length()-1);
		}
		
		
	    public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
	     HashSet<String> set=new HashSet<String>();
	     for (Text val : values) {
	    	 
	        String k=val.toString();
	        set.add(k);
	        
	        
	      }
	     
	     if(set.size()==1){
	    	 for(String s: set)
	    		 context.write(new Text(s), new Text());
	    	 return;
	     }
	     
	      HashMap<String,String> bigset=combine(new ArrayList<String>(set));
	      context.write(key, new Text(convert(bigset)));
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
    Job job = new Job(conf, "combine consolidated global tokens");
    job.setJarByClass(CombineConsolidatedGlobalTokens.class);
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
