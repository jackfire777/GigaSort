package gigasort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class GigaSortReducer extends Reducer<LongWritable, IntWritable, LongWritable, NullWritable> {

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