package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;
import mqttsn.gateway.utils.Utils;

import java.io.UnsupportedEncodingException;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNWillMsgUpd extends MqttSNMessage {
    //MqttSN WILLMSGUPD message
    private String willMsg;

    public MqttSNWillMsgUpd() {
        msgType = WILLMSGUPD;
    }

    public MqttSNWillMsgUpd(byte[] data) {
        msgType = WILLMSGUPD;
        try {
            willMsg = new String(data, 2, data[0] - 2, Utils.STRING_ENCODING);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] toBytes(){
        int length = 2 + willMsg.length();
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;
        System.arraycopy(willMsg.getBytes(), 0, data, 2, willMsg.length());
        return data;
    }

    public String getWillMsg() {
        return willMsg;
    }

    public void setWillMsg(String willMsg) {
        this.willMsg = willMsg;
    }
}
