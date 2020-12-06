package edu.rmit.cosc2367.s3773303.FirstMaven;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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

 
public class inmapper{

 

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
  {
	  HashMap<String, Integer> count = new HashMap<String, Integer>();

		private static final Logger LOG = Logger.getLogger(TokenizerMapper.class);

 

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            StringTokenizer itr = new StringTokenizer(value.toString());

 

            while (itr.hasMoreTokens()) 
            {
                String token = itr.nextToken();
                String length = token.toString();
                
                if(count.containsKey(length)) 
                {
                    int sum = (int) count.get(length) + 1;
                    count.put(length, sum);
                }
                else {
                    count.put(length, 1);
                }
            }
            LOG.setLevel(Level.DEBUG);
            LOG.info("The mapper of task 3 of Jeyakaran Karnan" + " s3773303" );
        }

 

        public void cleanup(Context context) throws IOException, InterruptedException 
        {
            Iterator<Map.Entry<String, Integer>> temp = count.entrySet().iterator();

 

            while(temp.hasNext()) {
                Map.Entry<String, Integer> entry = temp.next();
                String keyVal = entry.getKey()+"";
                Integer countVal = entry.getValue();

 

                context.write(new Text(keyVal), new IntWritable(countVal));
            }
        }
  }

 

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
        private IntWritable result = new IntWritable();

 
		private static final Logger LOG = Logger.getLogger(TokenizerMapper.class);


        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
            LOG.setLevel(Level.DEBUG);
            LOG.info("The reducer of task 3 of Jeyakaran Karnan" + " s3773303" );
        }
    }

 

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(inmapper.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        //job.setNumReduceTasks(1);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
