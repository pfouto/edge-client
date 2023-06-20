package messaging;

import babel.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import utils.HybridTimestamp;

import java.io.IOException;

public class ResponseMessage extends ProtoMessage {
    public static final short ID = 402;
    private final long opId;
    private final HybridTimestamp hlc;
    private final byte[] data;

    public ResponseMessage(long opId, HybridTimestamp hlc, byte[] data) {
        super(ID);
        this.opId = opId;
        this.hlc = hlc;
        this.data = data;
    }

    public long getOpId() {
        return opId;
    }

    public HybridTimestamp getHlc() {
        return hlc;
    }

    public byte[] getData() {
        return data;
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<ResponseMessage>() {
        @Override
        public void serialize(ResponseMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.getOpId());
            if(msg.getHlc() != null) {
                out.writeBoolean(true);
                HybridTimestamp.serializer.serialize(msg.hlc, out);
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
        public ResponseMessage deserialize(ByteBuf buff) throws IOException {
            long opId = buff.readLong();
            HybridTimestamp hlc = null;
            if(buff.readBoolean())
                hlc = HybridTimestamp.serializer.deserialize(buff);

            int size = buff.readInt();
            byte[] data = null;
            if(size > 0) {
                data = new byte[size];
                buff.readBytes(data);
            }

            return new ResponseMessage(opId, hlc, data);
        }
    };

}
