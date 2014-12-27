package com.jd.bdp.mysql.tracker;

import com.jd.bdp.magpie.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tracker.HandlerMagpieSimple;

/**
 * Created by hp on 14-12-27.
 */
public class LogSimple {

    private static Logger logger = LoggerFactory.getLogger(LogSimple.class);

    public static void main(String[] args) throws Exception {
        logger.info("start simple");
        HandlerMagpieSimple handler = new HandlerMagpieSimple();
        logger.info("new handler");
        Topology topo = new Topology(handler);
        logger.info("new topology");
        topo.run();
    }

}
