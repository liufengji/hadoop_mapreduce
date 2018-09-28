package com.victor.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WordCountCombine extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
						  Context context) throws IOException, InterruptedException {

		// 汇总
		int count = 0;
		// <a,1><a,1><a,1>---><a,3>
		for (IntWritable value : values) {
			count += value.get();
		}

		// 输出
		context.write(key, new IntWritable(count));
	}

}
