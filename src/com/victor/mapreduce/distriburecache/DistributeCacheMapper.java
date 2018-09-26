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

		// ��ȡ�����ļ�
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File("pd.txt")), "UTF-8"));

		String line;
		// 01 С��
		while (StringUtils.isNotEmpty(line = reader.readLine())) {

			// �и�
			String[] fields = line.split("\t");

			pdMap.put(fields[0], fields[1]);
		}

		// �ر���Դ
		reader.close();
	}

	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// ��ȡ���������ƴ��

		// ��ȡһ��
		// 1001 01 1
		String line = value.toString();

		// �и�
		String[] fields = line.split("\t");

		String pid = fields[1];

		String pName = pdMap.get(pid);

		// ƴ��
		k.set(line + "\t" + pName);
		
		// д��
		context.write(k, NullWritable.get());
	}
}
