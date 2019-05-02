package org.cent.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Demo of using kafka-client consumer
 *
 */
public class App 
{
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main( String[] args ) throws InterruptedException {
        logger.info("Go testing kafka consumer");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "group-test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("test"));

        // seekToBeginning() work lazily only when poll() or position() are called.
        // it means heartbeat started, and assignment happened
        consumer.poll(100);
        consumer.seekToBeginning(consumer.assignment());
        logger.info("Reset all partition consumer offset to beginning");

        long begin = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
        long end;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("ConsumerRecord {offset=%d, key=%s, value=%s, timestamp=%s}\n",
                        record.offset(), record.key(), record.value(), new Timestamp(record.timestamp()).toString());
            }

            TimeUnit.SECONDS.sleep(10);
            end = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
            if (end-begin >= 1) {
                logger.info("End testing and closing the consumer, after "+(end-begin)+" minutes.");
                consumer.close();
                break;
            }
        }
    }
}
