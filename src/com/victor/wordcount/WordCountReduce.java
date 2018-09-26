package com.victor.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// reduce�׶�
/*
 * KEYIN, 		Text, 		����
 * VALUEIN, 	IntWritable	���ʸ���
 * KEYOUT, 		Text, 		����
 * VALUEOUT		IntWritable	���ʸ���
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	// ��дreduce
	// hello 1
	// yinggu 1
	// yinggu 1
	// world  1
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		String k = key.toString();
		
		// 1 ����
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		
		// 2 ���
		context.write(key, new IntWritable(count));
	}
}
