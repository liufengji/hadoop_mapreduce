package com.victor.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

public class TestDecompress {

	public static void main(String[] args) throws Exception {
		decompress("e:/log.txt.gz");
	}

	// ��ѹ��
	private static void decompress(String filename) throws FileNotFoundException, IOException {
		
		// 1 �ж��Ƿ��н�ѹ���ķ���
		CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
		
		CompressionCodec codec = factory.getCodec(new Path(filename));
		
		if (null == codec) {
			System.out.println("not find " + codec);
			return;
		}
		
		// 2 ��ȡ������
		CompressionInputStream cis = codec.createInputStream(new FileInputStream(filename));
		
		// 3 ��ȡ�����
		FileOutputStream fos = new FileOutputStream(new File(filename + ".decode"));
		
		// 4 ���ĶԿ���
		IOUtils.copyBytes(cis, fos, 1024*1024*5, false);
		
		// 5 �ر���Դ
		cis.close();
		fos.close();
		
	}
}
