package gigasort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GigaSortPartitioner extends Partitioner<LongWritable, IntWritable>{
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