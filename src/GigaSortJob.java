package gigasort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GigaSortJob {
	public static void main(String[] args){
		try{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "NumSort");

			job.setJarByClass(GigaSortJob.class);
			job.setMapperClass(GigaSortMapper.class);
			job.setReducerClass(GigaSortReducer.class);
			job.setPartitionerClass(GigaSortPartitioner.class);

			job.setMapOutputValueClass(IntWritable.class);
		
			job.setNumReduceTasks(16);
		
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(NullWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);
		} catch (IOException e) {
			System.err.println(e.getMessage());
		} catch (InterruptedException e) {
			System.err.println(e.getMessage());
		} catch (ClassNotFoundException e) {
			System.err.println(e.getMessage());
		}
	}
}
