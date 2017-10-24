package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNAdvertise  extends MqttSNMessage {

    private int gwId;
    private long duration;

    public MqttSNAdvertise() {
        msgType = ADVERTISE;
    }


    public MqttSNAdvertise(byte[] data) {
        msgType = ADVERTISE;
        gwId = (data[2] & 0xFF);
        duration = ((data[3] & 0xFF) << 8) + (data[4] & 0xFF);
    }

    @Override
    public byte[] toBytes() {
        int length = 5;
        byte[] data = new byte[length];
        data[0] = (byte) length;
        data[1] = (byte) msgType;
        data[2] = (byte) gwId;
        data[3] = (byte) ((duration >> 8) & 0xFF);
        data[4] = (byte) (duration & 0xFF);
        return data;
    }

    public int getGwId() {
        return gwId;
    }

    public void setGwId(int gwId) {
        this.gwId = gwId;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }
}