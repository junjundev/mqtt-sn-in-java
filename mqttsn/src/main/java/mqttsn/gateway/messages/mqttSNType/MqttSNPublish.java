package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;
import mqttsn.gateway.utils.Utils;

import java.io.UnsupportedEncodingException;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNPublish extends MqttSNMessage {
    //MqttSN PUBLISH fields
    private boolean  dup;
    private int      qos;
    private	boolean retain;
    private int topicIdType;

    private byte[] byteTopicId;
    private int msgId;
    private byte[] pubData = null;

    //The form of TopicId (or short topic name) that depends on topicIdType.
    //Maybe either an int or a String.
    private int topicId = 0;
    private String shortTopicName = "";

    public MqttSNPublish() {
        msgType = PUBLISH;
    }

    public MqttSNPublish(byte[] data) {
        msgType = PUBLISH;
        dup = ((data[2] & 0x80) >> 7 != 0);
        qos = (data[2] & 0x60) >> 5;
        if(qos == 3) qos = -1;
        retain = ((data[2] & 0x10) >> 4 != 0);
        topicIdType = (data[2] & 0x03);

        byteTopicId = new byte[2];
        byteTopicId[0] = data[3];
        byteTopicId[1] = data[4];

        try {
            if (topicIdType == SHORT_TOPIC_NAME)
                shortTopicName = new String(byteTopicId, Utils.STRING_ENCODING);
            else if(topicIdType == NORMAL_TOPIC_ID || topicIdType == PREDIFINED_TOPIC_ID){
                topicId = ((byteTopicId[0] & 0xFF) << 8) + (byteTopicId[1] & 0xFF);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        msgId   = ((data[5] & 0xFF) << 8) + (data[6] & 0xFF);
        int plength = (data[0] & 0xFF) - 7;
        pubData = new byte[plength];
        System.arraycopy(data, 7, pubData, 0, plength);
    }

    @Override
    public byte[] toBytes() {
        int flags = 0;
        if(dup) {
            flags |= 0x80;
        }
        if(qos == -1) {
            flags |= 0x60;
        } else if(qos == 0) {

        } else if(qos == 1) {
            flags |= 0x20;
        } else if(qos == 2) {
            flags |= 0x40;
        } else {
            throw new IllegalArgumentException("Unknown QoS value: " + qos);
        }
        if(retain) {
            flags |= 0x10;
        }
        if(topicIdType == NORMAL_TOPIC_ID){
            //do nothing
        }else if (topicIdType == PREDIFINED_TOPIC_ID){
            flags |= 0x01;
        }else if (topicIdType == SHORT_TOPIC_NAME){
            flags |= 0x02;
        }else {
            throw new IllegalArgumentException("Unknown topic id type: " + topicIdType);
        }

        int length = 7 + pubData.length;
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;
        data[2] = (byte)flags;

        byteTopicId = new byte[2];
        if (topicIdType == SHORT_TOPIC_NAME)
            byteTopicId = shortTopicName.getBytes();
        else if(topicIdType == NORMAL_TOPIC_ID){
            byteTopicId[0] = (byte)((topicId >> 8) & 0xFF);
            byteTopicId[1] = (byte) (topicId & 0xFF);
        }else
            throw new IllegalArgumentException("Unknown topic id type: " + topicIdType);
        System.arraycopy(byteTopicId, 0, data, 3, byteTopicId.length);
        data[5] = (byte)((msgId >> 8) & 0xFF);
        data[6] = (byte) (msgId & 0xFF);
        System.arraycopy(pubData, 0, data, 7, pubData.length);
        return data;
    }

    public boolean isDup() {
        return dup;
    }

    public void setDup(boolean dup) {
        this.dup = dup;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public int getTopicIdType() {
        return topicIdType;
    }

    public void setTopicIdType(int topicIdType) {
        this.topicIdType = topicIdType;
    }

    public byte[] getData() {
        return pubData;
    }

    public void setData(byte[] data) {
        this.pubData = data;
    }

    public byte[] getByteTopicId() {
        return byteTopicId;
    }

    public void setByteTopicId(byte[] byteTopicId) {
        this.byteTopicId = byteTopicId;
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

    public String getShortTopicName() {
        return shortTopicName;
    }

    public void setShortTopicName(String shortTopicName) {
        this.shortTopicName = shortTopicName;
    }
}
