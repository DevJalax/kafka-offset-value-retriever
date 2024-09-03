import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaOffsetValueRetriever {

    public static void main(String[] args) {
        // Kafka consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto commit

        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        
        // Define the topic and partition
        String topic = "my-topic";
        int partition = 0; // specify your partition
        long offset = 15; // specify your desired offset

        // Assign the topic partition to the consumer
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(topicPartition));

        // Seek to the specified offset
        consumer.seek(topicPartition, offset);

        // Get the value of the message at the specified offset
        ConsumerRecord<String, String> record = consumer.get(topicPartition, offset);
        String value = record.value();
        System.out.println("Message value: " + value);

        // Close the consumer
        consumer.close();
    }
}
