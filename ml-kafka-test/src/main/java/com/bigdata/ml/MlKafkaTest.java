package com.bigdata.ml;
import java.io.IOException;

import com.bigdata.ml.kafka.SupplyProductEntity;
import com.bigdata.ml.kafka.kafkaProducer;

public class MlKafkaTest {
   public static void main(String[] args){
	   String topic="test-topic";
	   SupplyProductEntity entity=new SupplyProductEntity();
	   entity.setProductId("1");
	   entity.setCompanyName("大数据公司");
	   entity.setProductInfo("大数据，机器学习，海量处理");
	   //还有好多字段，大家自己写一下
	   String brokerList="10.165.16.134:2181,10.161.90.120:2181,10.163.249.21:2181";
	   try {
		kafkaProducer.producer(brokerList, topic, entity);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
}
