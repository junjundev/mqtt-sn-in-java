package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNRegack extends MqttSNMessage {
    //MqttSN REGACK fields
    private int msgId;
    private int returnCode;
    private int topicId;

    public MqttSNRegack() {
        msgType = REGACK;
    }

    public MqttSNRegack(byte[] data) {
        msgType = REGACK;
        topicId = ((data[2] & 0xFF) << 8) + (data[3] & 0xFF);
        msgId = ((data[4] & 0xFF) << 8) + (data[5] & 0xFF);
        returnCode = (data[6] & 0xFF);
    }

    @Override
    public byte[] toBytes(){
        int length = 7;
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;
        data[2] = (byte)((topicId >> 8) & 0xFF);
        data[3] = (byte)(topicId & 0xFF);
        data[4] = (byte)((msgId >> 8) & 0xFF);
        data[5] = (byte)(msgId & 0xFF);
        data[6] = (byte)(returnCode);
        return data;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public int getTopicId() {
        return topicId;
    }

    public void setTopicId(int topicId) {
        this.topicId = topicId;
    }

    public int getMsgId() {
        return msgId;
    }

    public void setMsgId(int msgId) {
        this.msgId = msgId;
    }
}
