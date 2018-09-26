package com.victor.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// map�׶�
/*
 * KEYIN, 		map������  LongWritable  �к�
 * VALUEIN, 	map��values����	Text  һ������
 * KEYOUT, 		map�����		Text			����
 * VALUEOUT		map����� 		IntWritable		���ʵĸ���
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text k = new Text();
	IntWritable v = new IntWritable(1);

	// ����map����
	// key���к�
	// value:
	// hello world yinggu yinggu
	// yinggu yinggu
	// hadoop
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1 ��ȡһ������
		String line = value.toString();

		// 2 �и�
		String[] words = line.split(" ");

		// 3 ���ͳ�ȥ
		for (String word : words) {

			k.set(word.getBytes());

			context.write(k, v);
		}
	}
}
