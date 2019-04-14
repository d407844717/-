package com.bigdata.ml.main;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.bigdata.ml.file.SupplyProductFile;
import com.ml.smart.hdfs.HDFSFileOperations;
import com.ml.smart.model.SupplyProduct;
import com.ml.smart.service.SupplyProductService;

//@ContextConfiguration(locations = { "classpath:spring-context.xml" }) 
public class StartTask {
   public static void main(String[] args) throws IOException{
	 // new StartTask().writeText();
	   ClassPathXmlApplicationContext ctx=new ClassPathXmlApplicationContext("classpath:spring-context.xml");
	   SupplyProductService supplyProductService=(SupplyProductService)ctx.getBean("supplyProductService");
	 //  System.out.println(supplyProductService.getSupplyProdict(1, 20));
	   float totalsize=0;
	   String lastpath="rawData/"+"supplyproduct-"+System.currentTimeMillis();	  
	   for(int i=0;i<1298;i++){
	        
	          List<SupplyProduct>  supplyProducts=supplyProductService.getSupplyProdict(i, 1000);

	          String line="";
	          for(int j=0;j<supplyProducts.size();j++){
	        	  SupplyProduct supplyProduct=supplyProducts.get(j);
	        	  String info=replaceBlank(supplyProduct.getProductName());
	        	  String companyName=supplyProduct.getCompanyName();
	        	  if(info!=""&&info!=null&&companyName!=null){
	        	    line+=supplyProduct.getProductId()+"[]"+supplyProduct.getCompanyName()+"[]"+supplyProduct.getDirection()+"[]"+info+"\r\n";
	        	  }
	          }
	        //  System.out.println(i);
	          ByteArrayInputStream is = new ByteArrayInputStream(line.getBytes());
	          totalsize=totalsize+line.getBytes().length;
	          if(totalsize>=13421772){
	        	  lastpath="rawData/"+"supplyproduct-"+System.currentTimeMillis(); 
	        	  totalsize=0;
	          }
	          System.out.println("B:"+totalsize+",B:"+line.getBytes().length+",num:"+i);
	          HDFSFileOperations.addFileStream(is,Boolean.TRUE,lastpath);
	        //  SupplyProductFile.writeFile(line);
	          
	    	}
   }
   public static String replaceBlank(String str) {
	           String dest = "";
	           if (str!=null) {
	               Pattern p = Pattern.compile("\\s*|\t|\r|\n");
	               Matcher m = p.matcher(str);
	               dest = m.replaceAll("");
	           }
	           return dest;
	       }
}
