package typeGoldStandard;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FreebaseTypesStatistics {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
/*
 * This file was written to extract freebase types statistics, similar
 * to type-experiments/dbpediaTypeStatistics.txt. The result is a two-field tab
 * separated file of the form <freebase-type> <count-as-long-value>
 * Also, at the end of the file, we have a TOTAL also in the same format as above.
 * We will add this last line serially after downloading the mapreduce output file.
 * Note that because of the large sizes of freebase-type files, we could not process
 * serially, unlike dbpedia.
 */
	  

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	
    	
        	
	  		String[] nodes=line.toLowerCase().split("\t");
	  		if(nodes.length!=2) return;
  			context.write(new Text(nodes[1]), new Text("1"));
  			
  		
  				
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
   // private Text result = new Text();
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
     
    	
	     long sum=0;
	     for (Text val : values) {
	    	 sum++;
	    	 
	     }
	     context.write(key, new Text(Long.toString(sum)));
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
    Job job = new Job(conf, "freebase types statistics");
    job.setJarByClass(FreebaseTypesStatistics.class);
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
