package cn.odyssey.back.functions;

import cn.odyssey.back.utils.ConfigNames;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSourceBuilder {
    public FlinkKafkaConsumer<String> build(String topic) {
        Config config = ConfigFactory.load();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getString(ConfigNames.KAFKA_BOOTSTRAP_SERVERS));
        properties.put("auto.offset.reset", config.getString(ConfigNames.KAFKA_AUTO_OFFSET_RESET));
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
        return stringFlinkKafkaConsumer;
    }
}
