package com.songListen;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class songListenReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{	
	
	public void reduce(IntWritable trackID,
			Iterable<IntWritable> listenIDs,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
					throws IOException, InterruptedException
	{
			int trackIDSet = 0;
	
			for(IntWritable listenID : listenIDs) {				
			 trackIDSet+=listenID.get();
		}
		    IntWritable size = new IntWritable(trackIDSet);
				 context.write(trackID,size);
}
}
