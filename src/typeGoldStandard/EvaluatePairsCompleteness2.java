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

public class EvaluatePairsCompleteness2 {
/**
 * Meant for taking the output of EvalPC1
 * and producing the output:
 * <idx>[tab]<count>
 * The number of lines can then be used to directly deduce (to a lower bound) PC
 * The count is the number of times <idx> occurred in the original file. In a certain
 * sense, the <idx>s with lower counts are 'harder' to recover.
 * @author Mayank
 *
 */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  
	  
	  
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	  	//precautionary lower-case conversion
    	String line = value.toString().toLowerCase();
    	String[] fields=line.split("\t");
    	//came from the score file
    	if(fields.length!=4)
    		return;
    	else
    		context.write(new Text(fields[3]), new Text("d"));
			
    	
		
    	
		
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
	  public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	     long sum=0;
		  for(Text val: values)
			  sum++;
		  String s=Long.toString(sum);
		  context.write(key, new Text(s));
		     
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
    Job job = new Job(conf, "deduplicate pairs completeness file");
    job.setJarByClass(EvaluatePairsCompleteness2.class);
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
