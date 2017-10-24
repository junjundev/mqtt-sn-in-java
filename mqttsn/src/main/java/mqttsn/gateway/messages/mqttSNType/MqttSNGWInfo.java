package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNGWInfo extends MqttSNMessage {

    //MqttSN GWINFO fields
    private int gwId;

    public MqttSNGWInfo() {
        msgType = GWINFO;
    }

    public MqttSNGWInfo(byte[] data) {
        msgType = GWINFO;
        gwId = (data[2] & 0xFF);
    }

    @Override
    public byte[] toBytes() {
        int length = 3;
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;
        data[2] = (byte)gwId;
        return data;
    }

    public int getGwId() {
        return gwId;
    }

    public void setGwId(int gwId) {
        this.gwId = gwId;
    }
}
