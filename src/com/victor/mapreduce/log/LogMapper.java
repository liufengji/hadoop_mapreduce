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

		// 1 获取一行
		String line = value.toString();

		// // 2 切割
		// String[] fields = line.split(" ");

		// 3 清洗
		boolean result = parseLog(line, context);

		if (!result) {
			return;
		}

		k.set(line);

		// 4 输出合法的
		context.write(k, NullWritable.get());
	}

	// 数据清洗的业务逻辑
	private boolean parseLog(String line, Context context) {
		// 去除日志中字段长度小于等于11的日志。
		boolean result;

		// 截取
		String[] fields = line.split(" ");

		if (fields.length > 11) {
			
			// 正确
			result = true;
			
			context.getCounter("map", "parseLog_true").increment(1);
		} else {

			// 错误
			result = false;

			context.getCounter("map", "parseLog_false").increment(1);
		}

		return result;
	}

}
