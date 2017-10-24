package mqttsn.gateway.messages;

public abstract class MqttMessage {
    //MQTT 消息类型
    public final static int CONNECT 			= 1;
    public final static int CONNACK 			= 2;
    public final static int PUBLISH 			= 3;
    public final static int PUBACK 				= 4;
    public final static int PUBREC 				= 5;
    public final static int PUBREL 				= 6;
    public final static int PUBCOMP 			= 7;
    public final static int SUBSCRIBE 			= 8;
    public final static int SUBACK 				= 9;
    public final static int UNSUBSCRIBE 	 	= 10;
    public final static int UNSUBACK 			= 11;
    public final static int PINGREQ 			= 12;
    public final static int PINGRESP 			= 13;
    public final static int DISCONNECT 			= 14;

    protected int msgType;
    public final static int RETURN_CODE_CONNECTION_ACCEPTED = 0;
    public static final int    MAX_CLIENT_ID_LENGTH = 23;

    public MqttMessage() {

    }

    /**
     * 计算MQTT 消息长度，并且加密消息
     */
    protected byte[] encodeMsgLength(byte[] data) {
        int size = data.length - 1;
        int pos = 0;
        byte[] tmp = new byte[4];
        do {
            int digit = size % 128;
            size = size /128;
            if (size > 0) {
                digit = digit | 0x80; //digit 与0x80做或运算
            }
            tmp[pos++] = (byte) digit;
        } while (size > 0);
        byte[] buffer = new byte[data.length + pos];
        buffer[0] = data[0];
        System.arraycopy(tmp,0,buffer,1,pos);
        System.arraycopy(data,1,buffer,pos+1,data.length-1); //重置消息
        data = buffer;
        return data;
    }

    /**
     * 解码一个MQTT消息剩余长度字段并返回消息的剩余的字节数
     */
    protected long decodeMsglength(byte[] data) {
        byte digit;
        long msgLength = 0;
        int multiplier = 1;
        int offset = 1;
        do {
            digit = (byte)data[offset];
            msgLength += (digit & 0x7F) * multiplier;
            multiplier *= 128;
            offset++;
        } while ((digit & 0x80) != 0);
        return msgLength;
    }

    public abstract byte[] toBytes ();

    public int getMsgType() {
        return msgType;
    }

    public void setMsgType(int msgType) {
        this.msgType = msgType;
    }
}
