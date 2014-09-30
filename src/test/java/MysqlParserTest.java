import tracker.CanalEntry;
import tracker.EntryPrinter;
import tracker.MysqlTracker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by hp on 14-9-3.
 */
public class MysqlParserTest {

    private static String fileName = "EntryBytesCode.dat";

    public static void main(String []args)throws IOException{
        File datFile = new File(fileName);
        BufferedReader br = new BufferedReader(new FileReader(datFile));
        String entryString;
        while((entryString = br.readLine())!=null){
            byte[] entryByte = MysqlTracker.getByteArrayFromString(entryString);
            CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(entryByte);
            EntryPrinter.printEntry(entry);
        }
        br.close();
    }

}
