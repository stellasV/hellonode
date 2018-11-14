package com.test.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaExConsumer 
{
	//private static final Logger logger = Logger.getLogger(KafkaExConsumer.class);
	private static final String ConsumerTopic = "test-topic";
	
	public static void main(String[] args) throws Exception 
    {
		List<String> topicList = new ArrayList<>();
		topicList.add(ConsumerTopic);
		
		Properties consumerProp = new Properties();
	    consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
	    consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG, ConsumerTopic);
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafConsumer = new KafkaConsumer<String, String>(consumerProp);

        kafConsumer.subscribe(topicList);
        System.out.println("Subscribed to topic " + ConsumerTopic);

        try 
        {
            while (true) 
            {
            	ConsumerRecords<String, String> records = kafConsumer.poll(500);
                for (ConsumerRecord<String, String> record : records)
                {
                	System.out.println("offset=" + record.offset() + ", key=" + record.key() + ", value=" + record.value());
                }

                kafConsumer.commitSync();
              
            }
        } 
        catch (Exception ex) {  System.out.println(ex.getMessage()); } 
        finally
        {
            try { kafConsumer.commitSync();  } 
            finally {  kafConsumer.close();  }
        }
    }
	 

}
