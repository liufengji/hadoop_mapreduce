package com.victor.mapreduce.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	FlowBean v =  new FlowBean();
	
	// key 13726230503 �绰��
	// values FlowBean ��������
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {

		long sum_upflow = 0;
		long sum_downflow = 0;

		// 1 ����
		for (FlowBean flowBean : values) {
			sum_upflow += flowBean.getUpFlow();
			sum_downflow += flowBean.getDownFlow();
		}

		// �������
		v.set(sum_upflow, sum_downflow);
		
		// 2 ���
		context.write(key, v);

	}
	
}
