package mqttsn.gateway.messages.mqttSNType;

import mqttsn.gateway.messages.MqttSNMessage;
import mqttsn.gateway.utils.Utils;

import java.io.UnsupportedEncodingException;

/**
 * Created by jungao on 2017/10/19.
 */
public class MqttSNConnect extends MqttSNMessage {

    private boolean will;
    private boolean cleanSession;
    private String protocolId;
    private int duration;
    private String clientId;

    private String protocolName;
    private int protocolVersion;

    public MqttSNConnect(){
        msgType = CONNECT;
    }

    public MqttSNConnect(byte[] data) {
        msgType = CONNECT;
        will = ((data[2] & 0x08) >> 3 != 0);
        cleanSession = ((data[2] & 0x04) >> 2 !=0);
        duration = ((data[4] & 0xFF) << 8) + (data[5] & 0xFF);

        protocolName = "MQIsdp";
        protocolVersion = 3;

        byte[] byteClientId = new byte[data[0] - 6];
        System.arraycopy(data, 6, byteClientId, 0, byteClientId.length);
        try {
            clientId = new String(byteClientId, Utils.STRING_ENCODING);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] toBytes() {
        int length = 6 + clientId.length();
        byte[] data = new byte[length];
        data[0] = (byte)length;
        data[1] = (byte)msgType;
        data[2] = (byte)0x00;
        if(will)  data[2] |= 0x08;
        if(cleanSession) data[2] |= 0x04;
        data[3] = (byte)0x00;
        data[4] = (byte)((duration >> 8) & 0xFF);
        data[5] = (byte)(duration & 0xFF);
        System.arraycopy(clientId.getBytes(), 0, data, 6, clientId.length());
        return data;
    }

    public boolean isWill() {
        return will;
    }

    public void setWill(boolean will) {
        this.will = will;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public String getProtocolId() {
        return protocolId;
    }

    public void setProtocolId(String protocolId) {
        this.protocolId = protocolId;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public String getProtocolName() {
        return protocolName;
    }

    public void setProtocolName(String protocolName) {
        this.protocolName = protocolName;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

}
