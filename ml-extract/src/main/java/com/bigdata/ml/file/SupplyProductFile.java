package com.bigdata.ml.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SupplyProductFile {

   public static void writeFile(String data){
		 FileWriter fileWritter=null;
	   try{

		      File file =new File("/home/*/service/OriginalSupplyProduct.txt");

		      //if file doesnt exists, then create it
		      if(!file.exists()){
		       file.createNewFile();
		      }
              
		      //true = append file
		      fileWritter = new FileWriter("/home/*/service/OriginalSupplyProduct.txt",true);
		      fileWritter.write(data);
//		      BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
//		      bufferWritter.write(data);
//		      bufferWritter.close();

		      System.out.println("Done");

		     }catch(IOException e){
		      e.printStackTrace();
		     }finally{
		    	 try{
		    		 if(fileWritter!=null){
		    			 fileWritter.close();
		    		 }
		    		 }catch(IOException e){
		    			 e.printStackTrace();
		    		 }
		    	 }
		     }
   }
