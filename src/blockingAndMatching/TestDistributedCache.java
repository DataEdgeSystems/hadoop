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

public class TestDistributedCache {
	
	
	
/**
 * This class is only for doing distributed cache testing. First, we will ascertain
 * if the correct files made it into the cache without crashing. 
 * ---> This test was successful; things get loaded into the cache in the order
 * in which we put them on the command line.
 * 
 * Next, we will
 * make sure the types file (which is around 100 MB) can be read in the mapper
 * without crashing. We will also make sure the distributed cache is accessible from
 * the reducer.
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

	 
	  public static HashMap<String, HashSet<String>> map=null;	//freebase types referencing dbpedia types
	  public boolean isMapPopulated=false;
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	Path[] cacheFiles = context.getLocalCacheFiles();
	    String rankFile=cacheFiles[1].toString();
	      if(!isMapPopulated)
	    	{
	    	  isMapPopulated=true;
	    	  if(map==null)
	    	  {   map=new HashMap<String, HashSet<String>>(5000);
		    	  Scanner in=new Scanner(new FileReader(rankFile));
		    	  while(in.hasNextLine()){
		    		  String[] fields=in.nextLine().split("\t");
		    		  if(!map.containsKey(fields[0]))
		    			  map.put(fields[0], new HashSet<String>());
		    		  map.get(fields[0]).add(fields[1]);
		    	  }
		  		
		  		  in.close();
	    	  }
	    	}
	      context.write(new Text("success"), new Text());
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     
		  context.write(key, new Text());
		  Path[] cacheFiles = context.getLocalCacheFiles();
		   context.write(new Text(Integer.toString(cacheFiles.length)), new Text());
		   context.write(new Text(cacheFiles[0].toString()), new Text()); 
		   context.write(new Text(cacheFiles[1].toString()), new Text());
		  return;
	      
	      
	  		
	      
	      
	      
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
    Job job = new Job(conf, "freebase-dbpedia: distributed-cache test");
    DistributedCache.addCacheFile(new URI(args[2]),job.getConfiguration());
    DistributedCache.addCacheFile(new URI(args[3]),job.getConfiguration());
    job.setJarByClass(TestDistributedCache.class);
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
