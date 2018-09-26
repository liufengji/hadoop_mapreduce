package com.victor.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
//		yinggu  a.txt	3
//		yinggu  b.txt	2
//		yinggu  c.txt	2
		
		StringBuffer sb = new StringBuffer();
		
		for (Text value : values) {
			
			sb.append(value.toString().replace("\t", "-->") + "\t");
		}
		
		context.write(key, new Text(sb.toString()));
		
	}
}
