package tracker.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hp on 14-12-12.
 */
public class TrackerConf {
    //mysql conf
    public String username = "canal";
    public String password = "canal";
    public String address = "127.0.0.1";
    public int myPort = 3306;
    public long slaveId = 15879;
    //kafka conf
    public static String brokerList = "localhost:9092";//"12:9092,13.9092,14:9092"
    public static int kafkaPort = 9092;
    public static String zkKafka = "localhost:2181";
    public static String serializer = "kafka.serializer.DefaultEncoder";//default is byte[]
    public static String partitioner = "kafka.producer.DefaultPartitioner";
    public static String acks = "1";
    public static String topic = "test";//queue topic
    public static int partition = 0;
    public static List<String> topics = new ArrayList<String>();//distribute the multiple topic
    public static List<String> brokerSeeds = new ArrayList<String>();//"12,13,14"
    //zk conf
    public String zkServers = "127.0.0.1:2181";//"48:2181,19:2181,50:2181"
    public int timeout = 100000;
    public String rootPath = "/mysql_tracker";
    public String persisPath = rootPath + "/persis";
    public String minutePath = rootPath + "/minutes";
    //tracker conf
    public int batchsize = 10000;
    public int queuesize = 50000;
    public int sumBatch = 5 * batchsize;
    public int timeInterval = 1;
    public String filterRegex = ".*\\..*";
    public int minsec = 60;

//    static  {
//        brokerSeeds.add("127.0.0.1");
//    }

//    public void testInit() {
//        brokerSeeds.add("127.0.0.1");
//    }

    public void testInit() {
        username = "jd_data";
        password = "jd_data";
        address = "172.17.36.48";
        myPort = 3306;
        brokerList = "172.17.36.53:9092,172.17.36.54:9092,172.17.36.55:9092";
        brokerSeeds.add("172.17.36.53");
        brokerSeeds.add("172.17.36.54");
        brokerSeeds.add("172.17.36.55");
        kafkaPort = 9092;
        zkKafka = "172.17.36.60/kafka";
        topic = "mysql_log";
        zkServers = "172.17.36.60:2181,172.17.36.61:2181,172.17.36.62:2181";
    }
}
