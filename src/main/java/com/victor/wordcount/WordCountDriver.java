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

		// 1 获取job对象
		Configuration configuration = new Configuration();

		// 开启map端输出压缩
		configuration.setBoolean("mapreduce.map.output.compress", true);
		// 设置map端输出压缩方式
		configuration.setClass("mapreduce.map.output.compress.codec", DefaultCodec.class, CompressionCodec.class);


		Job job = Job.getInstance(configuration);

		// 2 加载驱动类
		job.setJarByClass(WordCountDriver.class);

		// 3 加载mapper和reducer类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);

		// 4 map端输出的key 和value 类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 最终输出的key 和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);



//		// 8 切片
//		job.setInputFormatClass(CombineTextInputFormat.class);
//		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
//		CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

		// 9 合并
//		job.setCombinerClass(WordCountCombine.class);
//		job.setCombinerClass(WordCountReduce.class);

		// 6 设置输入输出文件路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 设置reduce端输出压缩开启
		FileOutputFormat.setCompressOutput(job, true);

		// 设置压缩的方式
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);


		// 7 提交
		// job.submit();
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
