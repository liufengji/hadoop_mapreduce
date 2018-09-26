package com.victor.mapreduce.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KEYOUT: �绰���� VALUEOUT�� bean����
 * 
 * @author Administrator
 *
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	FlowBean v = new FlowBean();
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1 ��ȡһ������
		// 1363157985066 13726230503 00-FD-07-A4-72-B8:CMCC 120.196.100.82
		// i02.c.aliimg.com 24 27 2481 24681 200
		String line = value.toString();

		// 2 ��ȡ��\t��
		String[] fields = line.split("\t");

		// 3 ��װ���� 13726230503
		String phone = fields[1];
		// ��������
		long upflow = Long.parseLong(fields[fields.length - 3]);
		long downflow = Long.parseLong(fields[fields.length - 2]);
		
		// �������
		v.set(upflow, downflow);
		k.set(phone);

		// 4 ���
		context.write(k, v);
	}
	
}
