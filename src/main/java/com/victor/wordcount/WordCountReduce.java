package com.victor.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// reduce阶段
/*
 * KEYIN, 		Text, 		单词
 * VALUEIN, 	IntWritable	单词个数
 * KEYOUT, 		Text, 		单词
 * VALUEOUT		IntWritable	单词个数
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	// 重写reduce
	// hello 1
	// yinggu 1
	// yinggu 1
	// world  1
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
						  Context context) throws IOException, InterruptedException {

		String k = key.toString();

		// 1 汇总
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}

		// 2 输出
		context.write(key, new IntWritable(count));
	}
}
