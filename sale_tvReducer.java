package com.sale;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class sale_tvReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{	
	
	public void reduce(Text stateId,
			Iterable<IntWritable> unitIds,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
					throws IOException, InterruptedException
	{
			int unitIdSet = 0;
            
			for(IntWritable unitId : unitIds) {				
				unitIdSet+=unitId.get();
		}
		    IntWritable size = new IntWritable(unitIdSet);
				 context.write(stateId,size);
}
}
