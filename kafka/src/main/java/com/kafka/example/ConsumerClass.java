package com.kafka.example;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ConsumerClass extends Thread {

	KafkaConsumer<String, String> consumer;
	String TOPIC = "sampletopic";
	
	private void configureConsumer() {

		//TODO: Might need to adjust the properties based on testing.
		Properties props = new Properties();		
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id","groupnew5");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset","earliest");
		props.put("enable.auto.commit", "true");	
		
		consumer 	= new KafkaConsumer<>(props);
		

	}
	@Override
	public void run() {
		configureConsumer();
		consumer.subscribe(Collections.singletonList(TOPIC));
		
		while(true) {
			ConsumerRecords<String, String> consumerRecords =  consumer.poll(100);
//			System.out.println("Inside");
			if (consumerRecords == null || consumerRecords.count() == 0) {
				continue;
			}	
			for (ConsumerRecord<String,String> consumerRecord:consumerRecords) {
				
				String value= consumerRecord.value();
				System.out.println(consumerRecord.key() );
				
				ObjectMapper objectMapper = new ObjectMapper();
				
				try {
					Map<String,String> map = objectMapper.readValue(value, new TypeReference<Map<String,String>>() {});
					
					for(Map.Entry<String,String> a :map.entrySet()) {
						System.out.println(a.getValue() + " " + a.getKey());
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					System.out.println(e.getMessage());
				}
			}
			
		}
	}
	public static void main(String[] args) {
		
	
		ConsumerClass consumerClass = new ConsumerClass();
		consumerClass.start();
	}
}
