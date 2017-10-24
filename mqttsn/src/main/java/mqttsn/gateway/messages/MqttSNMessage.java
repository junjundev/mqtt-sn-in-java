package mqttsn.gateway.messages;

public abstract class MqttSNMessage {
    //MQTT-SN 消息类型
    public static final int ADVERTISE    	= 0x00;
    public static final int SEARCHGW     	= 0x01;
    public static final int GWINFO       	= 0x02;
    public static final int CONNECT      	= 0x04;
    public static final int CONNACK      	= 0x05;
    public static final int WILLTOPICREQ 	= 0x06;
    public static final int WILLTOPIC    	= 0x07;
    public static final int WILLMSGREQ   	= 0x08;
    public static final int WILLMSG      	= 0x09;
    public static final int REGISTER     	= 0x0A;
    public static final int REGACK       	= 0x0B;
    public static final int PUBLISH      	= 0x0C;
    public static final int PUBACK       	= 0x0D;
    public static final int PUBCOMP			= 0x0E;
    public static final int PUBREC       	= 0x0F;
    public static final int PUBREL       	= 0x10;
    public static final int SUBSCRIBE		= 0x12;
    public static final int SUBACK       	= 0x13;
    public static final int UNSUBSCRIBE  	= 0x14;
    public static final int UNSUBACK     	= 0x15;
    public static final int PINGREQ      	= 0x16;
    public static final int PINGRESP     	= 0x17;
    public static final int DISCONNECT   	= 0x18;
    public static final int WILLTOPICUPD 	= 0x1A;
    public static final int WILLTOPICRESP	= 0x1B;
    public static final int WILLMSGUPD   	= 0x1C;
    public static final int WILLMSGRESP   	= 0x1D;
    public static final int ENCAPSMSG       = 0xFE;

    protected int msgType;

    public final static int NORMAL_TOPIC_ID = 0;
    public final static int PREDIFINED_TOPIC_ID = 1;
    public final static int TOPIC_NAME = 0;
    public final static int SHORT_TOPIC_NAME = 2;

    public final static int RETURN_CODE_ACCEPTED = 0;
    public final static int RETURN_CODE_REJECTED_CONGESTION = 1;
    public final static int RETURN_CODE_INVALID_TOPIC_ID = 2;

    public MqttSNMessage() {
    }

    public abstract byte[] toBytes ();


    public int getMsgType() {
        return msgType;
    }

    public void setMsgType(int msgType) {
        this.msgType = msgType;
    }
}
