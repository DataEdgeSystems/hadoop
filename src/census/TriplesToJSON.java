package census;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriplesToJSON {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  //Meant for census
	  private Text reducekey = new Text();
	    private Text reducevalue = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	line=line.replaceAll("\\[", "");
    	line=line.replaceAll("\\]", "");
    	line=line.replaceAll("\\{", "");
    	line=line.replaceAll("\\}", "");
    	line=line.replaceAll(":", "");
    	line=line.replaceAll("\"", "");
    	
    	
        	
  		String[] nodes=line.toLowerCase().split("\t");
  		if(nodes.length!=3)
  			return;
		
  		
  		
		
		reducekey.set(nodes[0]);
		reducevalue.set(nodes[1]+"\t"+nodes[2]);
		context.write(reducekey, reducevalue);
		
			
  			
  		
  				
    	
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
   // private Text result = new Text();
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	Text result = new Text();
    	
    	String json="{"+"\t";
    	
        
     HashMap<String, HashSet<String>> f=new HashMap<String, HashSet<String>>();
     
    
     
    	  for(Text t: values) {
    		 
    		String[] nodes=null;
    		 
    		  String line=t.toString();
    		 
    		
			nodes=line.split("\t");
			if(nodes.length!=2)
				continue;
			
    	    
    	  String prop=nodes[0];
			if(!f.containsKey(prop))
				f.put(prop, new HashSet<String>());
			
			f.get(prop).add(nodes[1]);
			
    		
    	  }
    	  //f.put("subject", new HashSet<String>());
    	  //f.get("subject").add(key.toString());
		String subject="\"subject\":[\""+key.toString()+"\"]";	
		json=json+subject+"\t";
      for(String k: f.keySet()){
    	  HashSet<String> vals=f.get(k);
    	  if(vals.size()<=0)
    		  continue;
    	  String val="[";
    	  for(String v:vals)
    		  val+=("\""+v+"\", ");
    	  val=val.substring(0, val.length()-2);
    	  val=val+"]";
    	  json=json+"\""+k+"\":"+val+"\t";
      }
     
      json=json+"}";
      result.set(json);
      
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
    Job job = new Job(conf, "census triples to json new");
    job.setJarByClass(TriplesToJSON.class);
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
