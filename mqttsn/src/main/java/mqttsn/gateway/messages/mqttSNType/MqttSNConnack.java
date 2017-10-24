package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNConnack extends MqttSNMessage {

    //Mqtts CONNACK fields
    private int returnCode;

    public MqttSNConnack() {
        msgType = CONNACK;
    }

    public MqttSNConnack(byte[] data) {
        msgType = CONNACK;
        returnCode = (data[2] & 0xFF);
    }

    @Override
    public byte[] toBytes() {
        int length = 3;
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;
        data[2] = (byte)returnCode;
        return data;
    }

    public int getReturnCode() {
        return returnCode;
    }
    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }
}
