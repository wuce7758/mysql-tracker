import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;
import tracker.MysqlTracker;
import tracker.MysqlTrackerHBase;

import java.io.IOException;

/**
 * Created by hp on 14-9-3.
 */
public class MysqlTrackerTest {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(MysqlTrackerTest.class);
    public static void main(String []args) throws IOException{

        logger.info("test log4j");
        //MysqlTrackerHBase tracker = new MysqlTrackerHBase("canal", "canal", "127.0.0.1", 3306, Long.valueOf(1234));
        MysqlTracker tracker = new MysqlTracker("canal", "canal", "127.0.0.1", 3306, Long.valueOf(1234));
        tracker.mainProc();

    }

}
