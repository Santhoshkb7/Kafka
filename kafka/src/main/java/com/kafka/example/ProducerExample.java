package com.kafka.example;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerExample implements Runnable{


	KafkaProducer<String,String> producer ;
	String topic = "sampletopic";
	
	public void configureProducer() {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//	props.put("streams.parallel.flushers.per.partition", "false");

		 producer = new KafkaProducer<String,String>(props);
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		configureProducer();
		while(true) {
			
			String key = String.valueOf(UUID.randomUUID());
			String value = "{\"samplekey\":\""+key+"\"}";
			System.out.println("Key "+ key + " Value:" + value);
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);
			producer.send(producerRecord);
			System.out.println("Sent");
		}

	}
	
	public static void main(String[] args) {
		Thread thread = new Thread(new ProducerExample());
		thread.start();
	}

}
