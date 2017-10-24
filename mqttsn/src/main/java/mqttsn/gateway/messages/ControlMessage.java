package mqttsn.gateway.messages;
public class ControlMessage {
    //消息控制体类型
    public static final int CONNECTION_LOST     		= 1;
    public static final int WAITING_WILLTOPIC_TIMEOUT 	= 2;
    public static final int WAITING_WILLMSG_TIMEOUT 	= 3;
    public static final int WAITING_REGACK_TIMEOUT 	    = 4;
    public static final int CHECK_INACTIVITY			= 5;
    public static final int SEND_KEEP_ALIVE_MSG			= 6;
    public static final int SHUT_DOWN					= 7;

    public ControlMessage(){}

    private int msgType;

    public int getMsgType() {
        return msgType;
    }

    public void setMsgType(int msgType) {
        this.msgType = msgType;
    }

}
