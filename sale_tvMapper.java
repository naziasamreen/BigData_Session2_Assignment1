package com.sale;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class sale_tvMapper extends Mapper<Object, Text, Text, IntWritable> {
	//private static final byte[] NA = null;
	private static final Text onida = new Text("Onida");
	IntWritable unitId= new IntWritable();
	Text compId= new Text();
	Text stateId= new Text();
	Text prodId= new Text();

public void map(Object key, Text value,
	Mapper<Object, Text, Text, IntWritable>.Context context) 
	throws IOException, InterruptedException {

       String[] lineArray = value.toString().split("[|]");
       compId.set(lineArray[0]);
       prodId.set(lineArray[1]);
       unitId.set(Integer.parseInt(lineArray[2]));
       stateId.set(lineArray[3]);
      Text text1=new Text("NA");
       if (!(prodId.equals(text1)))
    		   {
              if(!(compId.equals(text1)))
              {
            	  if(compId.equals(onida))
       context.write(new Text(stateId),unitId);
              }
              }
       }
  }