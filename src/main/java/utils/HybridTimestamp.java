package utils;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

public class HybridTimestamp {

    private final long logical;
    private final int counter;

    public HybridTimestamp(long logical, int counter){
        this.logical = logical;
        this.counter = counter;
    }

    public long getLogical() {
        return logical;
    }

    public int getCounter() {
        return counter;
    }


    public HybridTimestamp max(HybridTimestamp other){
        if(isAfter(other)) return this;
        return other;
    }

    public boolean isAfter(HybridTimestamp other){
        return logical > other.logical || (logical == other.logical && counter > other.counter);
    }

    public static ISerializer<HybridTimestamp> serializer = new ISerializer<HybridTimestamp>() {

        @Override
        public void serialize(HybridTimestamp o, ByteBuf out) {
            out.writeLong(o.getLogical());
            out.writeInt(o.getCounter());
        }

        @Override
        public HybridTimestamp deserialize(ByteBuf in) {
            return new HybridTimestamp(in.readLong(), in.readInt());
        }
    };
}
