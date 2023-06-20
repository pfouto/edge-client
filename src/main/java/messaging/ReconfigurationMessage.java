package messaging;

import babel.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReconfigurationMessage extends ProtoMessage {
    private final List<Host> hosts;
    public static final short ID = 404;

    public final List<Host> getHosts() {
        return this.hosts;
    }
    public ReconfigurationMessage(List<Host> hosts) {
        super(ID);
        this.hosts = hosts;
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<ReconfigurationMessage>() {
        public void serialize(ReconfigurationMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.getHosts().size());
            for(Host host : msg.getHosts()) {
                Host.serializer.serialize(host, out);
            }
        }

        public ReconfigurationMessage deserialize(ByteBuf buff) throws IOException {
            int size = buff.readInt();
            List<Host> hosts = new ArrayList<>(size);
            for(int i = 0; i < size; i++) {
                hosts.add(Host.serializer.deserialize(buff));
            }
            return new ReconfigurationMessage(hosts);
        }
    };
}
