package messaging;

import babel.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import utils.Operation;

import java.io.IOException;

public class RequestMessage extends ProtoMessage {
    private final long opId;
    private final Operation op;
    public static final short ID = 401;

    public final long getOpId() {
        return this.opId;
    }

    public final Operation getOp() {
        return this.op;
    }

    public RequestMessage(long opId, Operation op) {
        super(ID);
        this.opId = opId;
        this.op = op;
    }

    @Override
    public String toString() {
        return "RequestMessage{" +
                "opId=" + opId +
                ", op=" + op +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<RequestMessage>() {
        public void serialize(RequestMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.getOpId());
            Operation.serializer.serialize(msg.getOp(), out);
        }

        public RequestMessage deserialize(ByteBuf buff) throws IOException {
            long id = buff.readLong();
            Operation op = Operation.serializer.deserialize(buff);
            return new RequestMessage(id, op);
        }
    };
}
