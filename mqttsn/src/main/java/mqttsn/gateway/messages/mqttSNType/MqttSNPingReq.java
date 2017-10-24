package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNPingReq extends MqttSNMessage {

    public MqttSNPingReq() {
        msgType = PINGREQ;
    }


    public MqttSNPingReq(byte[] data) {
        msgType = PINGREQ;
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
