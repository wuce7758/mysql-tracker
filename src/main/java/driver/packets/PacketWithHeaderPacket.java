package driver.packets;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.base.Preconditions;
import tracker.CanalToStringStyle;

public abstract class PacketWithHeaderPacket implements IPacket {

    protected HeaderPacket header;

    protected PacketWithHeaderPacket(){
    }

    protected PacketWithHeaderPacket(HeaderPacket header){
        setHeader(header);
    }

    public void setHeader(HeaderPacket header) {
        Preconditions.checkNotNull(header);
        this.header = header;
    }

    public HeaderPacket getHeader() {
        return header;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}
