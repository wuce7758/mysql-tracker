package kafka.driver.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.KafkaConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by hp on 14-12-12.
 */
public class KafkaSender {

    private Logger logger = LoggerFactory.getLogger(KafkaSender.class);

    private KafkaConf conf;
    private Producer<String, byte[]> producer;
    private int retrys = 100;

    public KafkaSender(KafkaConf cf) {
        conf = cf;
    }

    public void connect() {
        Properties prop = new Properties();
        prop.put("metadata.broker.list", conf.brokerList);
        prop.put("serializer.class", conf.serializer);//msg is string
        prop.put("key.serializer.class", conf.keySerializer);
        prop.put("partitioner.class", conf.partitioner);
        prop.put("request.required.acks", conf.acks);
        ProducerConfig pConfig = new ProducerConfig(prop);
        producer = new Producer<String, byte[]>(pConfig);
    }

    public void send(byte[] msg) {
        KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(conf.topic, null, msg);
        blockSend(keyMsg);
    }

    public void send(String topic, byte[] msg) {
        KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(topic, null, msg);
        blockSend(keyMsg);
    }

    public void send(List<byte[]> msgs) {
        List<KeyedMessage<String, byte[]>> keyMsgs = new ArrayList<KeyedMessage<String, byte[]>>();
        for(byte[] msg : msgs) {
            KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(conf.topic, null, msg);
            keyMsgs.add(keyMsg);
        }
        blockSend(keyMsgs);
    }

    public void send(String topic, List<byte[]> msgs) {
        List<KeyedMessage<String, byte[]>> keyMsgs = new ArrayList<KeyedMessage<String, byte[]>>();
        for(byte[] msg : msgs) {
            KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(topic, null, msg);
            keyMsgs.add(keyMsg);
        }
        blockSend(keyMsgs);
    }

    public void sendKeyMsg(List<KeyedMessage<String, byte[]>> keyMsgs) {
        blockSend(keyMsgs);
    }

    public void blockSend(List<KeyedMessage<String, byte[]>> keyMsgs) {
        boolean isAck = false;
        int retryKafka = 0;
        while (!isAck) {
            if(retryKafka >= retrys) {
                reconnect();
                logger.warn("retry times out, reconnect the kafka server......");
                retryKafka = 0;
            }
            retryKafka++;
            try {
                producer.send(keyMsgs);
                isAck = true;
            } catch (Exception e) {
                logger.warn("retrying sending... Exception:" + e.getMessage());
                delay(3);
            }
        }
    }

    public void blockSend(KeyedMessage<String, byte[]> keyMsg) {
        boolean isAck = false;
        int retryKafka = 0;
        while (!isAck) {
            if(retryKafka >= retrys) {
                reconnect();
                logger.warn("retry times out, reconnect the kafka server......");
                retryKafka = 0;
            }
            retryKafka++;
            try {
                producer.send(keyMsg);
                isAck = true;
            } catch (Exception e) {
                logger.warn("retrying sending... Exception:" + e.getMessage());
                delay(3);
            }
        }
    }

    private void delay(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if(producer != null) producer.close();
    }

    public void reconnect() {
        close();
        connect();
    }

    public boolean isConnected() {
        Properties prop = new Properties();
        prop.put("metadata.broker.list", conf.brokerList);
        prop.put("serializer.class", conf.serializer);//msg is string
        prop.put("key.serializer.class", conf.keySerializer);
        prop.put("partitioner.class", conf.partitioner);
        prop.put("request.required.acks", conf.acks);
        prop.put("send.buffer.bytes",  conf.sendBufferSize);
        ProducerConfig pConfig = new ProducerConfig(prop);
        Producer<String, byte[]> heartPro = null;
        try {
            heartPro = new Producer<String, byte[]>(pConfig);
            if(heartPro != null) heartPro.close();
        } catch (Exception e) {
            return false;
        }
        return true;
    }
}
