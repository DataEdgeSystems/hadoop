package blockingAndMatching;

import java.io.FileReader;
import java.io.IOException;
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

public class TypeBlocker {
	
	
	
/**
 * designed for generating a candidate set of pairs using type information alone.
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

	  public static HashMap<String, HashSet<String>> freebaseMap=null;	//freebase types referencing idys
	  public static HashMap<String, HashSet<String>> dbpediaMap=null;
	  public boolean isFreebaseMapPopulated=false;
	  public boolean isDbpediaMapPopulated=false;
	  
	  static String[] tokenizer={"/", ",", ":", ";", "\\(", "\\)", "\\.", 
			"\"", "_", "-", "#", "\\\\", "\\s+"};	//everything here will be replaced with space when processing.
		//as a safety measure, we convert the json object to lowercase.
	 // static int bkvThreshold=20;	//do not assign more than this many bkvs/entity (even after controlling for types)
	  ///static int index=1;
	  
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	boolean dbpedia=false;
    	if(line.split("\t")[0].contains("dbpedia"))
    		dbpedia=true;
    	else if(!line.split("\t")[0].contains("rdf.freebase.com"))
    		return;
    	
    	Path[] cacheFiles = context.getLocalCacheFiles();
	    String rankFile=cacheFiles[0].toString();
	    
	    if(dbpedia){
	      if(!isDbpediaMapPopulated)
	    	{
	    	  isDbpediaMapPopulated=true;
	    	  if(dbpediaMap==null)
	    	  {   dbpediaMap=new HashMap<String, HashSet<String>>(500);
		    	  Scanner in=new Scanner(new FileReader(rankFile));
		    	  while(in.hasNextLine()){
		    		  String[] fields=in.nextLine().split("\t");
		    		  if(!dbpediaMap.containsKey(fields[2]))
		    			  dbpediaMap.put(fields[2], new HashSet<String>());
		    		  dbpediaMap.get(fields[2]).add(fields[0]);
		    	  }
		  		
		  		  in.close();
	    	  }
	    	}
	    }else{
	    	if(!isFreebaseMapPopulated)
	    	{
	    	  isFreebaseMapPopulated=true;
	    	  if(freebaseMap==null)
	    	  {   freebaseMap=new HashMap<String, HashSet<String>>(5000);
		    	  Scanner in=new Scanner(new FileReader(rankFile));
		    	  while(in.hasNextLine()){
		    		  String[] fields=in.nextLine().split("\t");
		    		  if(!freebaseMap.containsKey(fields[1]))
		    			  freebaseMap.put(fields[1], new HashSet<String>());
		    		  freebaseMap.get(fields[1]).add(fields[0]);
		    	  }
		  		
		  		  in.close();
	    	  }
	    	}
	    }
    	
    	String json=new String("{");
    	String[] fields=line.split("\t");
    	HashSet<String> idys=new HashSet<String>();
    	
    	for(int i=2; i<fields.length-1; i++){
    		json+=(fields[i]+"\t");
    		
    		String[] attributes=fields[i].toLowerCase().split("\":\\[");
			if(attributes.length!=2)
				return;	//malformed attribute
			String prop=attributes[0];
			prop=prop.replaceAll("\"", "");
			String values=attributes[1];
			if(!values.contains("]"))
				return;	//attribute close bracket missing
			values=values.replaceAll("\\]","");
			String[] list=values.split("\", \"");
			
			if(prop.contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")){
				for(String l: list){
					String type=l.replaceAll("\"", "");
					if(dbpedia){
						if(dbpediaMap.containsKey(type))
							for(String idy:dbpediaMap.get(type))
								idys.add(idy);
					}else{
						if(freebaseMap.containsKey(type))
							for(String idy:freebaseMap.get(type))
								idys.add(idy);
					}
				}
			}
    	}
    	if(idys.size()==0)
    		return;
    	
    	json=json+"}";
    	if(dbpedia)
    		json="dbpedia-instance\t"+json;
    	else
    		json="freebase-instance\t"+json;
    	
    			for(String idy: idys)
    				context.write(new Text(idy), new Text(json));
    				
    	
    
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public static double datasetLimit=100;
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      HashSet<String> dataset1=new HashSet<String>();	//store jsons from freebase
	      HashSet<String> dataset2=new HashSet<String>();	//store jsons from dbpedia
	 
	      for (Text val : values) {
	        String v=val.toString();
	        String type=v.split("\t")[0];
	    	
	        if(type.equals("freebase-instance"))
	        		if(dataset1.size()<datasetLimit)
	        			dataset1.add(v);
	        	
	        else if(type.equals("dbpedia-instance"))
	        	if(dataset2.size()<datasetLimit)
	        			dataset2.add(v);
	        	
	        
	        if(dataset1.size()*dataset2.size()>=10000)
	        	break;
	    	
	      }
	      
	      if(dataset1.size()==0||dataset2.size()==0)
	    	  return;
	      
	      
	      for(String d1: dataset1)
	    	  
	    	   for(String d2: dataset2)
	    		  
		    	  
		    	 context.write(new Text(d1), new Text(d2));
	  
	      
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
    Job job = new Job(conf, "freebase-dbpedia: Type-blocker");
    DistributedCache.addCacheFile(new URI(args[2]),job.getConfiguration());
 //   DistributedCache.addCacheFile(new URI(args[3]),job.getConfiguration());
    job.setJarByClass(TypeBlocker.class);
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
