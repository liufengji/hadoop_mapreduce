package com.victor.mapreduce.distriburecache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributeCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	Map<String, String> pdMap = new HashMap<String, String>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {

		// 读取缓存文件
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File("pd.txt")), "UTF-8"));

		String line;
		// 01 小米
		while (StringUtils.isNotEmpty(line = reader.readLine())) {

			// 切割
			String[] fields = line.split("\t");

			pdMap.put(fields[0], fields[1]);
		}

		// 关闭资源
		reader.close();
	}

	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 读取订单表及表的拼接

		// 获取一行
		// 1001 01 1
		String line = value.toString();

		// 切割
		String[] fields = line.split("\t");

		String pid = fields[1];

		String pName = pdMap.get(pid);

		// 拼接
		k.set(line + "\t" + pName);
		
		// 写出
		context.write(k, NullWritable.get());
	}
}
