package gigasort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GigaSortMapper extends	Mapper<LongWritable, Text, LongWritable, IntWritable> {
	
	private Long value;
	private IntWritable tally = new IntWritable();
	private LongWritable val = new LongWritable();
	
	/*
	 *  Input: Line of file, each line contains one Long
	 *  Output: <Long, 1>
	 */
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		this.value = Long.parseLong(value.toString());
		this.val.set(this.value);
		tally.set(1);
		context.write(val, tally);
	}
}