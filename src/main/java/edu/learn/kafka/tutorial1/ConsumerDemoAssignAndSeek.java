package edu.learn.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignAndSeek {

    public static final String TOPIC = "first_topic";
    public static Logger log = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String GROUP_ID = "my-fourth-application";


    public static void main(String[] args) {
        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);


        //assign and seek used to replay data or fetch specific message

        //assign
        TopicPartition topicPartitionReadFrom = new TopicPartition(TOPIC, 0);
        long offsetToReadFrom = 15l;
        kafkaConsumer.assign(Arrays.asList(topicPartitionReadFrom));

        //seek
        kafkaConsumer.seek(topicPartitionReadFrom, offsetToReadFrom);

        int noOfMsgToRead = 5;
        boolean keepOnReading = true;
        int noOfMessagesRead=0;
        //poll for new data
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                noOfMessagesRead++;
                log.info("key {}, value {}, partition {}, offset {}"
                        , record.key(), record.value(), record.partition(), record.offset());
                if(noOfMessagesRead >= noOfMsgToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
    }
}
