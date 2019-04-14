package com.bigdata.ml.kafka;
import org.apache.avro.io.*;   
import org.apache.avro.specific.SpecificDatumWriter;  
import kafka.javaapi.producer.Producer;  
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  
import java.io.ByteArrayOutputStream;  
import java.io.IOException;  
import java.util.Properties; 
import com.bigdata.ml.entity.SalesPurchasing;
import com.bigdata.ml.IKAnalyzer.IKAnalyzerSupplyProduct;
/*
 * 调用或者加载这个jar，发送数据到kafka（1、进行avro序列化，然后进行ik的切词，然后发送到kafka）
 */
public class kafkaProducer {
   /***
    * 
    * @param brokerlist  zk的地址
    * @param topics    zk的topic
    * @param entity
    * @throws IOException
    */
   public static void producer(String brokerlist,String topics,SupplyProductEntity entity) throws IOException{
	  if(entity.getCompanyName()!=null&&entity.getProductInfo()!=null&&entity.getCompanyName()!=""&&entity.getProductInfo()!=""){
	     Producer<String,byte[]> producers=createProducer(brokerlist);
	     SalesPurchasing supplyProduct=SalesPurchasing.newBuilder().build();
	     String productInfo=IKAnalyzerSupplyProduct.startIKAnalyzer(entity.getProductInfo());
	     if(productInfo!=""){	    	
	    	 supplyProduct.setProductId(entity.getProductId());
	    	 supplyProduct.setProductName(entity.getProductName());
	    	 supplyProduct.setPrice(entity.getPrice());
	    	 supplyProduct.setCategory(entity.getCategory());
	    	 supplyProduct.setProductContacts(entity.getProductContacts());
	    	 supplyProduct.setProductInfo(entity.getProductInfo());
	    	 supplyProduct.setProductInfoik(productInfo);
	    	 supplyProduct.setContactPhone(entity.getContactPhone());
	    	 supplyProduct.setCompanyName(entity.getCompanyName());
	    	 supplyProduct.setAddress(entity.getAddress());
	    	 supplyProduct.setSpecification(entity.getSpecification());
	    	 supplyProduct.setQuantity(entity.getQuantity());
	    	 supplyProduct.setPacking(entity.getPacking());
	    	 supplyProduct.setCreatorUser(entity.getCreatorUser());
	    	 supplyProduct.setDirection(entity.getDirection());
	    	 DatumWriter<SalesPurchasing> userDatumWriter = new SpecificDatumWriter<SalesPurchasing>(SalesPurchasing.class); 
	    	 ByteArrayOutputStream out = new ByteArrayOutputStream();  
	         BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);  
	         userDatumWriter.write(supplyProduct, encoder);  
	         encoder.flush();  
	         out.close(); 
	         byte[] serializedBytes = out.toByteArray();  
	         System.out.println("Sending message in bytes : " + serializedBytes);  
	         KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>(topics, serializedBytes);  
	         producers.send(message);  
	         //producers.close();
	     }
	  }
   }
   private static Producer<String,byte[]> producer=null;
   private static Producer<String,byte[]> createProducer(String brokerlist){
	    if(producer==null){
	    	Properties props=new Properties();
	    	//props.put("zookeeper.connect", "10.165.16.134:2181,10.161.90.120:2181,10.163.249.21:2181");
	    	props.put("metadata.broker.list", brokerlist);
	    	props.put("serializer.class", "kafka.serializer.DefaultEncoder"); 
	    	ProducerConfig config=new ProducerConfig(props);
	    	producer=new Producer<String,byte[]>(config);
	    }
	    return producer;
   }
  public static void main(String[] args) throws IOException{
	  for(int i=1295475500;i<1295475700;i++){
		  SupplyProductEntity entity=new SupplyProductEntity();
		  String j=Integer.toString(i);
		  entity.setProductId(j);
		  entity.setCompanyName("companyname"+"i");
		  entity.setProductInfo("1.需要props.putserializer.classkafka.serializer.DefaultEncoder这样数据传输才会用byte[]的方式2.schema中fields的type要有null，这样当字段值为空时，能够正常运行，否在不设置null，并且字段值为空时就会抛异常。");
	      entity.setDirection(j);
	      kafkaProducer.producer("10.163.249.12:6667,10.163.247.155:6667", "smart_supplyproduct", entity);
	  }
  }
}
