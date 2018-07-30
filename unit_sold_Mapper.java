package com.tv_set;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class tv_setMapper extends Mapper<Object, Text, Text, IntWritable> {
	//private static final byte[] NA = null;
	// private final static IntWritable one = new IntWritable(1);
	IntWritable unitId= new IntWritable();
	Text compId= new Text();;
	Text prodId= new Text();

public void map(Object key, Text value,
	Mapper<Object, Text, Text, IntWritable>.Context context) 
	throws IOException, InterruptedException {

       String[] lineArray = value.toString().split("[|]");
       compId.set(lineArray[0]);
       prodId.set(lineArray[1]);
       unitId.set(Integer.parseInt(lineArray[2]));
      Text text1=new Text("NA");
       if (!(prodId.equals(text1)))
    		   {
              if(!(compId.equals(text1)))
       context.write(new Text(compId),unitId);
       }
       }
  }