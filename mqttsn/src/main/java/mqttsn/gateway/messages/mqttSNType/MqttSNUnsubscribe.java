package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.MqttSNException;
import mqttsn.gateway.messages.MqttSNMessage;
import mqttsn.gateway.utils.Utils;

import java.io.UnsupportedEncodingException;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNUnsubscribe extends MqttSNMessage {
    private boolean  dup;
    private int topicIdType;

    private int   msgId = 0;
    private byte[] byteTopicId;
    private String topicName = "";
    private int predefinedTopicId = 0;
    private String shortTopicName = "";

    public MqttSNUnsubscribe() {
        msgType = UNSUBSCRIBE;
    }

    /**
     * MqttsUnubscribe constructor.Sets the appropriate message type and constructs
     * a MqttSN UNSUBSCRIBE message from a received byte array.
     * @param data: The buffer that contains the UNSUBSCRIBE message.
     * @throws MqttSNException
     */
    public MqttSNUnsubscribe(byte[] data) throws MqttSNException {
        msgType = UNSUBSCRIBE;
        dup = ((data[2] & 0x80) >> 7 != 0);
        topicIdType = (data[2] & 0x03);
        msgId   = ((data[3] & 0xFF) << 8) + (data[4] & 0xFF);

        int length = (data[0] & 0xFF)-5;
        byteTopicId = new byte[length];

        try {
            switch (topicIdType){
                case TOPIC_NAME:
                    System.arraycopy(data, 5, byteTopicId, 0, length);
                    topicName = new String(byteTopicId, Utils.STRING_ENCODING);
                    break;

                case PREDIFINED_TOPIC_ID:
                    if(length != 2){
                        throw new MqttSNException("Wrong format. Predefined topic id must be 2 bytes long.");
                    }
                    byteTopicId[0] = data[5];
                    byteTopicId[1] = data[6];
                    predefinedTopicId = ((byteTopicId[0] & 0xFF) << 8) + (byteTopicId[1] & 0xFF);
                    break;
                case SHORT_TOPIC_NAME:
                    if(length != 2)
                        throw new MqttSNException("Wrong format. Short topic name must be 2 bytes long.");
                    System.arraycopy(data, 5, byteTopicId, 0, byteTopicId.length);
                    shortTopicName = new String(byteTopicId,Utils.STRING_ENCODING);
                    break;

                default:
                    throw new MqttSNException("Unknown topic id type: " + topicIdType);
            }
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

   @Override
    public byte[] toBytes(){
        int flags = 0;
        if(dup) {
            flags |= 0x80;
        }
        if(topicIdType == TOPIC_NAME){
            byteTopicId = new byte[topicName.length()];
            System.arraycopy(topicName.getBytes(), 0, byteTopicId, 0, byteTopicId.length);
        }else if (topicIdType == PREDIFINED_TOPIC_ID){
            flags |= 0x01;
            byteTopicId = new byte[2];
            byteTopicId [0] = (byte)((predefinedTopicId >> 8) & 0xFF);
            byteTopicId [1] = (byte) (predefinedTopicId & 0xFF);
        }else if (topicIdType == SHORT_TOPIC_NAME){
            flags |= 0x02;
            byteTopicId = new byte[2];
            System.arraycopy(shortTopicName.getBytes(), 0, byteTopicId, 0, byteTopicId.length);
        }else {
            throw new IllegalArgumentException("Unknown topic id type: " + topicIdType);
        }

        int length = 5 + byteTopicId.length;
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;
        data[2] = (byte)flags;
        data[3] = (byte)((msgId >> 8) & 0xFF);
        data[4] = (byte) (msgId & 0xFF);
        System.arraycopy(byteTopicId, 0, data, 5, byteTopicId.length);
        return data;
    }


    public boolean isDup() {
        return dup;
    }

    public void setDup(boolean dup) {
        this.dup = dup;
    }

    public int getTopicIdType() {
        return topicIdType;
    }

    public void setTopicIdType(int topicIdType) {
        this.topicIdType = topicIdType;
    }

    public int getMsgId() {
        return msgId;
    }

    public void setMsgId(int msgId) {
        this.msgId = msgId;
    }

    public byte[] getByteTopicId() {
        return byteTopicId;
    }

    public void setByteTopicId(byte[] byteTopicId) {
        this.byteTopicId = byteTopicId;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPredefinedTopicId() {
        return predefinedTopicId;
    }

    public void setPredefinedTopicId(int predefineTopicId) {
        this.predefinedTopicId = predefineTopicId;
    }

    public String getShortTopicName() {
        return shortTopicName;
    }

    public void setShortTopicName(String shortTopicName) {
        this.shortTopicName = shortTopicName;
    }
}
