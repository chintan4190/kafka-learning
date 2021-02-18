package edu.learn.kafka.tutorial1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

    private static final String TOPIC = "first_topic";
    public static Logger log = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String GROUP_ID = "my-sixth-application";

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();

    }

    private void run() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runnable runnable = new ConsumerRunnable(countDownLatch);
        Thread thread = new Thread(runnable);
        thread.start();
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("shutdown hook");
            ((ConsumerRunnable)runnable).shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.info("application interrupted");
        }finally {
            log.info("application closing");
        }
    }


    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> kafkaConsumer;
        private Logger log = LoggerFactory.getLogger(ConsumerRunnable.class);


        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Properties properties = new Properties();
                    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
                    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
                    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        log.info("key {}, value {}, partition {}, offset {}"
                                , record.key(), record.value(), record.partition(), record.offset());
                    }
                    //subscribe consumer to our topic
                    kafkaConsumer.subscribe(Collections.singleton(TOPIC));
                }
            } catch (WakeupException e) {
                log.error("wake up excetion");
            } finally {
                kafkaConsumer.close();
                latch.countDown();
            }

        }

        public void shutdown() {
            kafkaConsumer.wakeup();
        }
    }
}
