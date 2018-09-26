package com.victor.mapreduce.outputformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// ��ȡһ��
		String line = value.toString();
		
		k.set(line+"\r\n");
		
		context.write(k, NullWritable.get());
	}
}
