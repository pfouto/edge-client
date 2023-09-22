package messaging;

import babel.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import utils.EngageOperation;
import utils.Operation;

import java.io.IOException;

public class EngageRequestMessage extends ProtoMessage {
    private final long opId;
    private final EngageOperation op;
    public static final short ID = 401;

    public final long getOpId() {
        return this.opId;
    }

    public final EngageOperation getOp() {
        return this.op;
    }

    public EngageRequestMessage(long opId, EngageOperation op) {
        super(ID);
        this.opId = opId;
        this.op = op;
    }

    @Override
    public String toString() {
        return "EngageRequestMessage{" +
                "opId=" + opId +
                ", op=" + op +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<EngageRequestMessage>() {
        public void serialize(EngageRequestMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.getOpId());
            EngageOperation.serializer.serialize(msg.getOp(), out);
        }

        public EngageRequestMessage deserialize(ByteBuf buff) throws IOException {
            long id = buff.readLong();
            EngageOperation op = EngageOperation.serializer.deserialize(buff);
            return new EngageRequestMessage(id, op);
        }
    };
}
