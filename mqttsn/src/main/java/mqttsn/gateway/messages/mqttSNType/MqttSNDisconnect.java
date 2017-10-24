package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNDisconnect extends MqttSNMessage {


    public MqttSNDisconnect() {
        msgType = DISCONNECT;
    }


    public MqttSNDisconnect(byte[] data) {
        msgType = DISCONNECT;
    }


    @Override
    public byte[] toBytes() {
        int length = 2;
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;
        return data;
    }

}
