package messaging;

import babel.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import utils.Clock;
import utils.HybridTimestamp;

import java.io.IOException;

public class EngageResponseMessage extends ProtoMessage {
    public static final short ID = 402;
    private final long opId;
    private final Clock clock;
    private final byte[] data;

    public EngageResponseMessage(long opId, Clock clock, byte[] data) {
        super(ID);
        this.opId = opId;
        this.clock = clock;
        this.data = data;
    }

    public long getOpId() {
        return opId;
    }

    public Clock getClock() {
        return clock;
    }

    public byte[] getData() {
        return data;
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<EngageResponseMessage>() {
        @Override
        public void serialize(EngageResponseMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.getOpId());
            if(msg.getClock() != null) {
                out.writeBoolean(true);
                Clock.serializer.serialize(msg.clock, out);
            } else
                out.writeBoolean(false);

            if(msg.getData() != null) {
                out.writeInt(msg.getData().length);
                out.writeBytes(msg.getData());
            } else {
                out.writeInt(0);
            }
        }

        @Override
        public EngageResponseMessage deserialize(ByteBuf buff) throws IOException {
            long opId = buff.readLong();
            Clock clock = null;
            if(buff.readBoolean())
                clock = Clock.serializer.deserialize(buff);

            int size = buff.readInt();
            byte[] data = null;
            if(size > 0) {
                data = new byte[size];
                buff.readBytes(data);
            }

            return new EngageResponseMessage(opId, clock, data);
        }
    };

}
