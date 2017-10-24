package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNWillTopicResp extends MqttSNMessage {

    public MqttSNWillTopicResp() {
        msgType = WILLTOPICRESP;
    }


    public MqttSNWillTopicResp(byte[] data){
        msgType = WILLTOPICRESP;
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
