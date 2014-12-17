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
    public static String zk = "localhost:2181";
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
    public int sumBatch = 5 * batchsize;
    public int timeInterval = 1;
    public String filterRegex = ".*\\..*";
    public int minsec = 60;

    static  {
        brokerSeeds.add("127.0.0.1");
    }
}
