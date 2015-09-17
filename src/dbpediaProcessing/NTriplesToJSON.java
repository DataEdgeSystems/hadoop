package dbpediaProcessing;

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

public class NTriplesToJSON {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  //meant for dbpedia 
	  private Text reducekey = new Text();
	    private Text reducevalue = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	try {
    	String line = value.toString();
    	
    	if(line.substring(0,1).equals("#"))
    		return;
    	String[] fields=line.toLowerCase().split("> ");
    	if(fields.length!=3&&fields.length!=4)
			return;
    	
    	String[] nodes=new String[3];
    	nodes[0]=fields[0]+">";
    	nodes[1]=fields[1]+">";
    	if(fields.length==3)
    		nodes[2]=fields[2].substring(0, fields[2].length()-2);
    	else if(fields.length==4)
    		nodes[2]=fields[2]+">";
    	String v=nodes[2];
			if(v.contains("\"@")){
				if(v.contains("\"@en")){
					reducekey.set(nodes[0]);
					reducevalue.set(nodes[1].toString()+"\t"+nodes[2].toString());
					context.write(reducekey, reducevalue);
				}
					
			}
			else{
				reducekey.set(nodes[0].toString());
				reducevalue.set(nodes[1].toString()+"\t"+nodes[2].toString());
				context.write(reducekey, reducevalue);
			}
  	} catch (Exception e) {
  		
  		e.printStackTrace();
  	}
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
   
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	Text result = new Text();
    	try{
    	String json="{"+"\t";
    	
        
     HashMap<String, HashSet<String>> f=new HashMap<String, HashSet<String>>();
     
     
     
    	  for(Text t: values) {
    		  
    		String[] nodes=null;
    		  int m=0;
    		  try{
    			m=1;
    		  String line=t.toString();
    		 
    		//System.out.println(key+line);
    		  m=2;
			nodes=line.split("\t");
			if(nodes.length!=2)
				continue;
			m=3;
    	    
    	  String prop=nodes[0];
			if(!f.containsKey(prop))
				f.put(prop, new HashSet<String>());
			String g=nodes[1].replaceAll("\\[", "");
			g=g.replaceAll("\\]", "");
			f.get(prop).add(g);
			
    		}catch(Exception e){
    			context.write(new Text("error code"+m+" nodes length "+nodes.length), result);
    			}
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
      if(json.contains("\"@en"))
    	  context.write(key, result);}
    	catch(Exception e){e.printStackTrace();}
      
    	  
    
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
    Job job = new Job(conf, "dbpedia: ntriples to json new");
    job.setJarByClass(NTriplesToJSON.class);
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
