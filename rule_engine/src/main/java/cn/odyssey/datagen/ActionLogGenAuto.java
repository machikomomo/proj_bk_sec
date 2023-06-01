package cn.odyssey.datagen;

import cn.odyssey.engine.beans.LogBean;
import cn.odyssey.engine.utils.ConfigNames;
import com.alibaba.fastjson.JSON;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Properties;

/**
 * kafka_topic 为 detail_action_log，同步数据存储到clickhouse的表 momo_detail
 */
public class ActionLogGenAuto {
    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getString(ConfigNames.KAFKA_BOOTSTRAP_SERVERS));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = "detail_action_log";

        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
                    while (true) {
                        String value = JSON.toJSONString(getLog());
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
                        producer.send(record);
                        try {
                            Thread.sleep(RandomUtils.nextInt(500, 600));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }


    }

    public static LogBean getLog() {
        LogBean logBean = new LogBean();
        // 生成的账号形如： 9999 left pad
        String account = StringUtils.leftPad(RandomUtils.nextInt(1, 10000) + "", 6, "0");
        logBean.setAccount(account);
        logBean.setAppId("cn.momo.applicationA");
        logBean.setAppVersion("2.5");
        logBean.setCarrier("中国移动");
        // deviceId直接用account
        logBean.setDeviceId(account);
        logBean.setIp("10.102.36.88");
        logBean.setLatitude(RandomUtils.nextDouble(10.0, 52.0));
        logBean.setLongitude(RandomUtils.nextDouble(120.0, 160.0));
        logBean.setDeviceType("mi6");
        logBean.setNetType("5G");
        logBean.setOsName("android");
        logBean.setOsVersion("7.5");
        logBean.setReleaseChannel("小米应用市场");
        logBean.setResolution("2048*1024");
        logBean.setTimeStamp(System.currentTimeMillis()); // 事件发生的时间
        logBean.setSessionId(RandomStringUtils.randomNumeric(10, 10));

        logBean.setEventId(RandomStringUtils.randomAlphabetic(1).toUpperCase());
        HashMap<String, String> properties = new HashMap<String, String>();
        for (int i = 0; i < RandomUtils.nextInt(1, 5); i++) {
            // range (1 2 3 4) 可以生成1-4个pv对
            // p1-p10;v1-v2
            properties.put("p" + RandomUtils.nextInt(1, 11), "v" + RandomUtils.nextInt(1, 3));
        }
        logBean.setProperties(properties);

        // 除了event以外一共有16个属性
        return logBean;
    }
}
