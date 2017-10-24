package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNPuback extends MqttSNMessage {
    //MqttSN PUBACK fields
    private int msgId;
    private int returnCode;
    private byte[] byteTopicId;


    //The form of TopicId maybe either an int or a String.
    private int topicId = 0;
    private String shortTopicName = null;

    public MqttSNPuback() {
        msgType = PUBACK;//check the conversion to bytes
    }

    public MqttSNPuback(byte[] data) {
        msgType = PUBACK;
        msgId = ((data[4] & 0xFF) << 8) + (data[5] & 0xFF);
        returnCode = (data[6] & 0xFF);
        if (returnCode == RETURN_CODE_INVALID_TOPIC_ID);
        topicId = ((data[2] & 0xFF) << 8) + (data[3] & 0xFF);
    }

    @Override
    public byte[] toBytes() {
        int length = 7;
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;

        byteTopicId = new byte[2];
        if (this.topicId != 0){
            byteTopicId[0] = (byte)((topicId >> 8) & 0xFF);
            byteTopicId[1] = (byte) (topicId & 0xFF);
        }else if(this.shortTopicName != null)
            byteTopicId = shortTopicName.getBytes();

        System.arraycopy(byteTopicId, 0, data, 2, byteTopicId.length);
        data[4] = (byte)((msgId >> 8) & 0xFF);
        data[5] = (byte) (msgId & 0xFF);
        data[6] = (byte)returnCode;
        return data;
    }


    public int getMsgId() {
        return msgId;
    }

    public void setMsgId(int msgId) {
        this.msgId = msgId;
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

    public String getShortTopicName() {
        return shortTopicName;
    }

    public void setShortTopicName(String shortTopicName) {
        this.shortTopicName = shortTopicName;
    }

    public byte[] getByteTopicId() {
        return byteTopicId;
    }

    public void setByteTopicId(byte[] byteTopicId) {
        this.byteTopicId = byteTopicId;
    }
}
