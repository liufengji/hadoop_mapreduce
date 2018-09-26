package com.victor.mapreduce.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {

	public static void main(String[] args) throws Exception {

		// 1 ��ȡjob
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 ����������
		job.setJarByClass(FlowDriver.class);

		// 3 ����map��reduce��
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);

		// 4 ����map�����key��value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 5 �������������key��value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// 6 ��������������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 �ύjob
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
	
}
