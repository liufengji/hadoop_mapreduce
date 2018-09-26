package com.victor.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {

		// 1 ��ȡjob����
		Configuration configuration = new Configuration();
		
		// ����map�����ѹ��
		configuration.setBoolean("mapreduce.map.output.compress", true);
		// ����map�����ѹ����ʽ
		configuration.setClass("mapreduce.map.output.compress.codec", DefaultCodec.class, CompressionCodec.class);

		
		Job job = Job.getInstance(configuration);

		// 2 ����������
		job.setJarByClass(WordCountDriver.class);

		// 3 ����mapper��reducer��
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);

		// 4 map�������key ��value ����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 ���������key ��value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		
//		// 8 ��Ƭ
//		job.setInputFormatClass(CombineTextInputFormat.class);
//		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
//		CombineTextInputFormat.setMinInputSplitSize(job, 2097152);
		
		// 9 �ϲ�
//		job.setCombinerClass(WordCountCombine.class);
//		job.setCombinerClass(WordCountReduce.class);
		
		// 6 ������������ļ�·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// ����reduce�����ѹ������
		FileOutputFormat.setCompressOutput(job, true);
		
		// ����ѹ���ķ�ʽ
	    FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class); 

		
		// 7 �ύ
		// job.submit();
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
