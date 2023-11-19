package trend;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileDescriptor;
import java.io.FileReader;
import java.util.Properties;
import java.util.logging.Logger;

public class Producer {
    private static final Logger logger = Logger.getLogger(Producer.class.getName());
    public static final String topicName = "meetup_events";
    public static final String inputFilePath = "src/main/resources/meetup.json";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Integer counter = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    producer.send(new ProducerRecord<>(topicName, line));
                    logger.info("Sent 1 message to Kafka, total sent messages: " + ++counter);
                    Thread.sleep(1000);
                } catch (Exception e) {
                    logger.severe("Error: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.severe("Error decoding JSON: " + e.getMessage());
        }

        producer.close();
    }
}
