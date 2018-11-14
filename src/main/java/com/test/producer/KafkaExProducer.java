package com.test.producer;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class KafkaExProducer 
{
	//private static final Logger logger = Logger.getLogger(KafkaExProducer.class);
	private static final String producerTopic = "test-topic";
	private static Scanner scanner;
	
	public static void main(String[] args)
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        try
        {
        	scanner = new Scanner(System.in);
        	String line = scanner.nextLine();
        	while(line.equals("exit") == false) 
            {
        		 ProducerRecord data = new ProducerRecord<String, String>(producerTopic, line);
        		 Future<RecordMetadata> recordMetadata = producer.send(data);
        		 System.out.println("topic="+producerTopic + ", value=" + line);
        		 line = scanner.nextLine();
            }
        	
        }
        catch(Exception e) { System.out.println(e.getMessage());  }
        finally 
        {
        	scanner.close();
        	producer.close(); 
        }

    }
	

}
