package com.bigdata.ml.main;

import java.io.IOException;

import com.ml.smart.IKAnalyzer.IKAnalyzerProduct;
import com.ml.smart.hdfs.HDFSFileOperations;

public class StartIKAnalyzer {
	 public static void main(String[] args) throws IOException{
		 HDFSFileOperations.readFileStream("rawData");
//		 IKAnalyzerProduct.startIKAnalyzer("zhang[]yong[]我是好人");
	 }
}
