package com.jd.bdp.mysql.tracker;

import com.jd.bdp.magpie.Topology;
import tracker.HandlerNoParserMagpieHBase;
import tracker.utils.TrackerConfiger;

/**
 * Created by hp on 14-11-6.
 */
public class LocalLogTracker {

    public static void main(String[] args) throws Exception {
        TrackerConfiger cnf = new TrackerConfiger();
        //default config
        cnf.setUsername("canal");
        cnf.setPassword("canal");
        cnf.setAddress("127.0.0.1");
        cnf.setPort(3306);
        cnf.setSlaveId(Long.valueOf(2234));
        cnf.setHbaseRootDir("hdfs://localhost:9000/hbase");
        cnf.setHbaseDistributed("true");
        cnf.setHbaseZkQuorum("127.0.0.1");
        cnf.setHbaseZkPort("2181");
        cnf.setDfsSocketTimeout("180000");
        HandlerNoParserMagpieHBase handler = new HandlerNoParserMagpieHBase(cnf);
        Topology topology = new Topology(handler);
        topology.run();
    }

}
