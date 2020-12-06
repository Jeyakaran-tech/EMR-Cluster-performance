package edu.rmit.cosc2367.s3773303.FirstMaven;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.rmit.cosc2367.s3773303.FirstMaven.task1.IntSumReducer;
import edu.rmit.cosc2367.s3773303.FirstMaven.task1.TokenizerMapper;

public class partitioner 
{
	public static  class partMapper extends Mapper<Object, Text, Text, IntWritable> 
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
	    private static final Logger LOG = Logger.getLogger( partPartitioner.class);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) 
			{
				int length = itr.nextToken().length();
				if(length >= 1 && length <=4) {
					word.set("small");
				}
				if(length >= 5 && length <=7) {
					word.set("medium");
				}
				if(length >= 8 && length <= 10) {
					word.set("long");
				}
				if(length > 10 ) {
					word.set("extra-long");
				}
				context.write(word, one);
				LOG.setLevel(Level.DEBUG);
	            LOG.info("The mapper of task 4 of Jeyakaran Karnan" + " s3773303" );
			}
		}
	}
	
	public static class partReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		private IntWritable result = new IntWritable();
	    private static final Logger LOG = Logger.getLogger( partPartitioner.class);

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			LOG.setLevel(Level.DEBUG);
            LOG.info("The reducer of task 4 of Jeyakaran Karnan" + " s3773303" );
		}

	}
	
	public static class partPartitioner extends Partitioner < Text, IntWritable >
	{
	    private static final Logger LOG = Logger.getLogger( partPartitioner.class);
	    
	   @Override
	   public int getPartition(Text key, IntWritable value, int numReduceTasks)
	   {
	       
	       
	       if(numReduceTasks == 0)
	       {
	           return 0;
	       }
	       
	       String length = key.toString(); 
	       if((length.equals("small")) || (length.equals("extra-long") ))
	       {
	           return 0;
	       }
	       else 
	       {
	           return 1 % numReduceTasks;
	       }
	      
	    }
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(partitioner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setPartitionerClass(partPartitioner.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
