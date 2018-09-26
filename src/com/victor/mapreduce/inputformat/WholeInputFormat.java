package com.victor.mapreduce.inputformat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeInputFormat extends FileInputFormat<NullWritable, BytesWritable>{

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
	
		// 不可以切片
		return false;
	}
	
	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		// 自定义recordreader
		WholeRecordReader reader = new WholeRecordReader();
		
		reader.initialize(split, context);
		
		return reader;
	}
}
