package com.victor.mapreduce.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MyFilterRecordWriter extends RecordWriter<Text, NullWritable> {

	FSDataOutputStream yingguout = null;
	FSDataOutputStream otherout = null;

	public MyFilterRecordWriter(TaskAttemptContext job) {
		//
		try {
			FileSystem fs = FileSystem.get(job.getConfiguration());

			yingguout = fs.create(new Path("e:/yinggu.log"));
			otherout = fs.create(new Path("e:/other.log"));

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		
		if (key.toString().contains("yinggu")) {
			yingguout.write(key.toString().getBytes());
		}else {
			otherout.write(key.toString().getBytes());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		// ¹Ø±Õ×ÊÔ´
		if (yingguout != null) {
			yingguout.close();
		}
		
		if (otherout != null) {
			otherout.close();
		}

	}

}
