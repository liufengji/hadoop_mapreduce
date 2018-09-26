package com.victor.mapreduce.table;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<TableBean> values, Context context)
			throws IOException, InterruptedException {
		// �ϲ����ű�
//		1001	01	1
//		1004	01	4
//		01	С��
		
		// ׼��
		ArrayList<TableBean> orderBeans = new ArrayList<>();
		TableBean pdBean = new TableBean();
		
		// ��������
		for (TableBean tableBean : values) {
			
			if ("0".equals(tableBean.getFlag())) {// ������
				
				TableBean orderBean = new TableBean();
				
				try {
					BeanUtils.copyProperties(orderBean, tableBean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
				
				orderBeans.add(orderBean);
			}else {// ��Ʒ��
				try {
					BeanUtils.copyProperties(pdBean, tableBean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}
		
		// �滻
		for (TableBean tableBean : orderBeans) {
			tableBean.setPname(pdBean.getPname());
			
			context.write(tableBean, NullWritable.get());
		}
	}
}
