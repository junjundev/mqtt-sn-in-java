package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;
import mqttsn.gateway.utils.Utils;

import java.io.UnsupportedEncodingException;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNWillTopicUpd extends MqttSNMessage {
    //MqttSN WILLTOPICUPD fields
    private int qos;
    private boolean retain;
    private String willTopic;

    public MqttSNWillTopicUpd() {
        msgType = WILLTOPICUPD;
    }

    public MqttSNWillTopicUpd(byte[] data) {
        msgType = WILLTOPICUPD;
        qos = (data[2] & 0x60) >> 5;
        retain = ((data[2] & 0x10) >> 4 !=0);
        try {
            willTopic = new String(data, 3, data[0] - 3, Utils.STRING_ENCODING);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

   @Override
    public byte[] toBytes(){
        int length = 3 + willTopic.length();
        byte[] data = new byte[length];
        int flags = 0;
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

        data[0] = (byte)length;
        data[1] = (byte)msgType;
        data[2] = (byte)flags;
        System.arraycopy(willTopic.getBytes(), 0, data, 3, willTopic.length());
        return data;
    }

    public int getQos() {
        return qos;
    }
    public void setQos(int qoS) {
        this.qos = qoS;
    }
    public boolean isRetain() {
        return retain;
    }
    public void setRetain(boolean retain) {
        this.retain = retain;
    }
    public String getWillTopic() {
        return willTopic;
    }
    public void setWillTopic(String willTopic) {
        this.willTopic = willTopic;
    }
}
