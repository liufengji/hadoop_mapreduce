package com.victor.mapreduce.table;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
	
	TableBean v = new TableBean();
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 0 ��ȡ�ļ�����
		FileSplit split = (FileSplit) context.getInputSplit();
		// ��ȡ�ļ�������
		String name = split.getPath().getName();
		
		// 1 ��ȡһ��
		String line = value.toString();
		
		if (name.startsWith("order")) {// ������
			
			// 2 �и�
			String[] fields = line.split("\t");
			
			// 3 ��װbean����
			v.setOrder_id(fields[0]);
			v.setP_id(fields[1]);
			v.setAmnout(Integer.parseInt(fields[2]));
			v.setPname("");
			v.setFlag("0");
			
			k.set(fields[1]);
			
		}else {// ��Ʒ��
			// 2 �и�
			String[] fields = line.split("\t");
			
			// 3��װbean����
			v.setOrder_id("");
			v.setP_id(fields[0]);
			v.setAmnout(0);
			v.setPname(fields[1]);
			v.setFlag("1");
			
			k.set(fields[0]);
		}

		// 4 д��
		context.write(k, v);
	}

	
	
}
