package com.victor.mapreduce.log;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {

		// 1 ��ȡһ��
		String line = value.toString();

		// // 2 �и�
		// String[] fields = line.split(" ");

		// 3 ��ϴ
		boolean result = parseLog(line, context);

		if (!result) {
			return;
		}

		k.set(line);

		// 4 ����Ϸ���
		context.write(k, NullWritable.get());
	}

	// ������ϴ��ҵ���߼�
	private boolean parseLog(String line, Context context) {
		// ȥ����־���ֶγ���С�ڵ���11����־��
		boolean result;

		// ��ȡ
		String[] fields = line.split(" ");

		if (fields.length > 11) {
			
			// ��ȷ
			result = true;
			
			context.getCounter("map", "parseLog_true").increment(1);
		} else {

			// ����
			result = false;

			context.getCounter("map", "parseLog_false").increment(1);
		}

		return result;
	}

}
