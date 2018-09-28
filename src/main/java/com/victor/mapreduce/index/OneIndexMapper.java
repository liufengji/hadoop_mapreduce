package com.victor.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

// k yinggu--a.txt
// v  个数
public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	String name;
	Text k = new Text();
	IntWritable v = new IntWritable(1);

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		// 获取名字
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		name = inputSplit.getPath().getName();

	}


	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		// 1 获取一行
		// yinggu pingping
		String line = value.toString();

		// 2 切割
		// yinggu
		// pingping
		String[] words = line.split(" ");

		// 3 遍历输出
		for (String word : words) {
			// yinggu--a.txt
			k.set(word + "--" + name);

			context.write(k, v);
		}

	}

}
