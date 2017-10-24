package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNWillMsgResp extends MqttSNMessage {

    public MqttSNWillMsgResp() {
        msgType = WILLMSGRESP;
    }

    /**
     * MqttSNWillMsgResp constructor.Sets the appropriate message type and constructs
     * a MqttSN WILLMSGRESP message from a received byte array.
     * @param data: The buffer that contains the WILLMSGRESP message.
     */
    public MqttSNWillMsgResp(byte[] data){
        msgType = WILLMSGRESP;
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
