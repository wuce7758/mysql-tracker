package tracker;

/**
 * Created by hp on 14-9-2.
 */
public class TrackerConfiger {


    private String username ;

    private String password ;

    private String address;

    private int port;

    private Long slaveId;

    private String hbaseString;

    public TrackerConfiger(String username, String password, String address, int port, Long slaveId) {
        this.username = username;
        this.password = password;
        this.address = address;
        this.port = port;
        this.slaveId = slaveId;
    }

    public TrackerConfiger(String username, String password, String address, int port, Long slaveId, String hbaseString) {
        this.username = username;
        this.password = password;
        this.address = address;
        this.port = port;
        this.slaveId = slaveId;
        this.hbaseString = hbaseString;
    }


    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public Long getSlaveId() {
        return slaveId;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setSlaveId(Long slaveId) {
        this.slaveId = slaveId;
    }

    public String getHbaseString() {
        return hbaseString;
    }

    public void setHbaseString(String hbaseString) {
        this.hbaseString = hbaseString;
    }

}
