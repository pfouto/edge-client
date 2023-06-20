package messaging;

import babel.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class PersistenceMessage extends ProtoMessage {
    private final long opId;
    public static final short ID = 403;

    public final long getOpId() {
        return this.opId;
    }

    public PersistenceMessage(long opId) {
        super(ID);
        this.opId = opId;
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<PersistenceMessage>() {
        public void serialize(PersistenceMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.getOpId());
        }

        public PersistenceMessage deserialize(ByteBuf buff) throws IOException {
            long id = buff.readLong();
            return new PersistenceMessage(id);
        }
    };
}
