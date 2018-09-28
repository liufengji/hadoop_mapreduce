package com.victor.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// map阶段
/*
 * KEYIN, 		map的输入  LongWritable  行号
 * VALUEIN, 	map的values输入	Text  一行内容
 * KEYOUT, 		map的输出		Text			单词
 * VALUEOUT		map的输出 		IntWritable		单词的个数
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text k = new Text();
	IntWritable v = new IntWritable(1);

	// 重新map方法
	// key：行号
	// value:
	// hello world yinggu yinggu
	// yinggu yinggu
	// hadoop
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1 获取一行数据
		String line = value.toString();

		// 2 切割
		String[] words = line.split(" ");

		// 3 发送出去
		for (String word : words) {

			k.set(word.getBytes());

			context.write(k, v);
		}
	}
}
