package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNSearchGW extends MqttSNMessage {

    //MqttSN SEARCHGW fields
    private int radius;

    public MqttSNSearchGW() {
        msgType = SEARCHGW;
    }

    public MqttSNSearchGW(byte[] data) {
        msgType = SEARCHGW;
        radius = (data[2] & 0xFF);
    }

    @Override
    public byte[] toBytes(){
        int length = 3;
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;
        data[2] = (byte)radius;
        return data;
    }

    public int getRadius() {
        return radius;
    }

    public void setRadius(int radius) {
        this.radius = radius;
    }
}
