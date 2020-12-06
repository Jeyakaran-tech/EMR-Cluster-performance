package edu.rmit.cosc2367.s3773303.FirstMaven;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.rmit.cosc2367.s3773303.FirstMaven.task1.TokenizerMapper;


public class vowel 
{
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static final Logger LOG = Logger.getLogger(TokenizerMapper.class);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				char Begin = itr.nextToken().charAt(0);
				if(Begin == 'A' || Begin == 'E' || Begin == 'I' || Begin == 'O' || Begin == 'U' || Begin == 'a' || Begin == 'e' || Begin == 'i' || Begin == 'o' || Begin == 'u') {
					word.set("vowels");
				}
				else {
					word.set("consonants");
				}
				context.write(word, one);
				LOG.setLevel(Level.DEBUG);
	            LOG.info("The mapper of task 2 of Jeyakaran Karnan" + " s3773303" );
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		private IntWritable result = new IntWritable();
		private static final Logger LOG = Logger.getLogger(TokenizerMapper.class);

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			LOG.setLevel(Level.DEBUG);
            LOG.info("The reducer of task 2 of Jeyakaran Karnan" + " s3773303" );
		}

	}

		public static void main(String[] args) throws Exception {
	// TODO Auto-generated method stub
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "word count");
			job.setJarByClass(vowel.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

}
