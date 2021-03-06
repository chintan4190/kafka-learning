package edu.learn.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerDemoWithKey {

    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class);


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            //create record
             String topic = "first_topic";
             String value = "hello_world_" + i;
             String key = "id_"+i;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            logger.info("key {}", key);

            //send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //record sent successfully
                        logger.info("received new metadata, topic {}, partition {}, offset {}, timestamp {}",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());

                    } else {
                        logger.error("error + " + e.getMessage());
                    }
                }
            }).get(); //block .send() to make it synchronous - not for PRODUCTION!!!!
        }
        //flush data
        producer.flush();

        producer.close();

    }
}
