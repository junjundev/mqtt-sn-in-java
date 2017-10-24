package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNWillMsgReq extends MqttSNMessage {

    public MqttSNWillMsgReq() {
        msgType = WILLMSGREQ;
    }

    /**
     * MqttSNWillMsgReq constructor.Sets the appropriate message type and constructs
     * a MqttSN WILLMSGREQ message from a received byte array.
     * @param data: The buffer that contains the WILLMSGREQ message.
     */
    public MqttSNWillMsgReq(byte[] data){
        msgType = WILLMSGREQ;
    }

    @Override
    public byte [] toBytes() {
        int length = 2;
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;
        return data;
    }
}
