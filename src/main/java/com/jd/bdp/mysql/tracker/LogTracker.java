package com.jd.bdp.mysql.tracker;

import com.jd.bdp.magpie.Topology;
import tracker.HandlerForMagpieHBase;
import tracker.utils.TrackerConfiger;

/**
 * Created by hp on 14-9-22.
 */
public class LogTracker {

    public static void main(String[] args) throws Exception {
        TrackerConfiger cnf = new TrackerConfiger();
        cnf.setUsername("jd_data");
        cnf.setPassword("jd_data");
        cnf.setAddress("172.17.36.48");
        cnf.setPort(3306);
        cnf.setSlaveId(Long.valueOf(2234));
        cnf.setHbaseRootDir("hdfs://BJ-YZH-1-H1-3650.jd.com:9000/hbase");
        cnf.setHbaseDistributed("true");
        cnf.setHbaseZkQuorum("BJ-YZH-1-H1-3660.jd.com,BJ-YZH-1-H1-3661.jd.com,BJ-YZH-1-H1-3662.jd.com");
        cnf.setHbaseZkPort("2181");
        cnf.setDfsSocketTimeout("180000");
        HandlerForMagpieHBase handler = new HandlerForMagpieHBase(cnf);
        Topology topology = new Topology(handler);
        topology.run();
    }

}
