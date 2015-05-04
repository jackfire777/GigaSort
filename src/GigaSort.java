package gigasort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GigaSort {

	public static class Map extends	Mapper<LongWritable, Text, LongWritable, IntWritable> {
		
		private Long value;
		private IntWritable tally = new IntWritable();
		private LongWritable val = new LongWritable();
		
		/*
		 *  Input: Line of file, each line contains one Long
		 *  Output: <Long, 1>
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			this.value = Long.parseLong(value.toString());
			this.val.set(this.value);
			tally.set(1);
			context.write(val, tally);
		}
	}
	
	public static class GigaPartitioner extends Partitioner<LongWritable, IntWritable>{
		private Long val;
		
		@Override
		public int getPartition(LongWritable key, IntWritable value, int numReducers) {
			/*this.val = key.get();
			long sizeOfPart = Long.MAX_VALUE/numReducers;
			for (int i = 0; i < numReducers; i++){
				if (val < (i+1)*sizeOfPart){
					return i;
				}
			}
			return 0;	//this should never happen*/
			
			
			this.val = key.get();
			return (int) ((double)val/Math.pow(2, 32)/Math.pow(2, 31)*numReducers);
		}
		
	}

	public static class Reduce extends Reducer<LongWritable, IntWritable, LongWritable, NullWritable> {

		private NullWritable nw = NullWritable.get();
		private int counter = 0;
		
		
		/*
		 * Input: <Long, [1,1,1,...]>
		 * e.g.   <28790877679087987698, [1]>
		 * 
		 * Output: 
		 * e.g.    
		 */
		protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for (@SuppressWarnings("unused") IntWritable num : values){
				if (counter % 1000 == 0){
					context.write(key, nw);
				}
				this.counter++;
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "NumSort");

		job.setJarByClass(GigaSort.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(GigaPartitioner.class);

		job.setMapOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(16);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}