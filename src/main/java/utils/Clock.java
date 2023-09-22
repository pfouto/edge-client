package utils;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.HashMap;
import java.util.Map;

public class Clock {
    private Map<Inet4Address, Integer> value;

    public Clock() {
        this.value = new HashMap<>();
    }

    private Clock(Map<Inet4Address, Integer> value) {
        this.value = value;
    }

    public Map<Inet4Address, Integer> getValue() {
        return value;
    }

    public void merge(Clock other) {
        for (Map.Entry<Inet4Address, Integer> entry : other.getValue().entrySet()) {
            value.merge(entry.getKey(), entry.getValue(), Integer::max);
        }
    }


    public static ISerializer<Clock> serializer = new ISerializer<Clock>() {
        @Override
        public void serialize(Clock clock, ByteBuf out) throws IOException {
            out.writeInt(clock.getValue().size());
            for (Map.Entry<Inet4Address, Integer> entry : clock.getValue().entrySet()) {
                out.writeBytes(entry.getKey().getAddress());
                out.writeInt(entry.getValue());
            }
        }

        @Override
        public Clock deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Map<Inet4Address, Integer> value = new HashMap<>();
            for (int i = 0; i < size; i++) {
                byte[] address = new byte[4];
                in.readBytes(address);
                value.put((Inet4Address) Inet4Address.getByAddress(address), in.readInt());
            }
            return new Clock(value);
        }
    };
}
