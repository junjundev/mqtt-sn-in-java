package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;
import mqttsn.gateway.utils.Utils;

import java.io.UnsupportedEncodingException;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNRegister extends MqttSNMessage {

    //MqttSN REGISTER fields
    private int topicId;
    private int msgId;
    private String topicName;

    public MqttSNRegister() {
        msgType = REGISTER;
    }

    public MqttSNRegister(byte[] data) {
        msgType = REGISTER;
        topicId = 0;//send by the client
        msgId = ((data[4] & 0xFF) << 8) + (data[5] & 0xFF);
        int tlen = (data[0] & 0xFF) - 6;
        byte[] byteTopicName = new byte[tlen];
        System.arraycopy(data, 6, byteTopicName, 0, tlen);
        try {
            topicName = new String(byteTopicName, Utils.STRING_ENCODING);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] toBytes() {
        int length = 6 + topicName.length();
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;
        data[2] = (byte)((topicId >> 8) & 0xFF);
        data[3] = (byte)(topicId & 0xFF);
        data[4] = (byte)((msgId >> 8) & 0xFF);
        data[5] = (byte)(msgId & 0xFF);
        System.arraycopy(topicName.getBytes(), 0, data, 6, topicName.length());
        return data;
    }

    public int getMsgId() {
        return msgId;
    }

    public void setMsgId(int msgId) {
        this.msgId = msgId;
    }

    public int getTopicId() {
        return topicId;
    }

    public void setTopicId(int topicId) {
        this.topicId = topicId;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }
}
