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
		// 合并两张表
//		1001	01	1
//		1004	01	4
//		01	小米

		// 准备
		ArrayList<TableBean> orderBeans = new ArrayList<>();
		TableBean pdBean = new TableBean();

		// 遍历拷贝
		for (TableBean tableBean : values) {

			if ("0".equals(tableBean.getFlag())) {// 订单表

				TableBean orderBean = new TableBean();

				try {
					BeanUtils.copyProperties(orderBean, tableBean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}

				orderBeans.add(orderBean);
			}else {// 产品表
				try {
					BeanUtils.copyProperties(pdBean, tableBean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}

		// 替换
		for (TableBean tableBean : orderBeans) {
			tableBean.setPname(pdBean.getPname());

			context.write(tableBean, NullWritable.get());
		}
	}
}
