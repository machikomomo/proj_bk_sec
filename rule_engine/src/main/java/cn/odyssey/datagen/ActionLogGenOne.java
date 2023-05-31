package cn.odyssey.datagen;

import cn.odyssey.engine.utils.ConfigNames;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ActionLogGenOne {
    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getString(ConfigNames.KAFKA_BOOTSTRAP_SERVERS));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        String topic = "action_log";
        String value = "hello!";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        producer.send(record);
        producer.flush();
    }
}
