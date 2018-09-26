package com.victor.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class TestComporess {

	public static void main(String[] args) throws Exception {
		compress("e:/log.txt","org.apache.hadoop.io.compress.BZip2Codec");
		// compress("e:/log.txt","org.apache.hadoop.io.compress.GzipCodec");
		// compress("e:/log.txt","org.apache.hadoop.io.compress.Lz4Codec");
		// compress("e:/test.txt","org.apache.hadoop.io.compress.DefaultCodec");

	}

	// ѹ������
	private static void compress(String filename, String method) throws ClassNotFoundException, IOException {

		// 1 ��ȡ������
		FileInputStream fis = new FileInputStream(new File(filename));

		// 2 ��ȡ�����
		Class classname = Class.forName(method);

		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(classname, new Configuration());

		FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));

		// 3 ���������װ��ѹ����
		CompressionOutputStream cos = codec.createOutputStream(fos);

		// 4 �Կ���
		IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);

		// 5 �ر���Դ
		fis.close();
		cos.close();
		fos.close();
	}

}
