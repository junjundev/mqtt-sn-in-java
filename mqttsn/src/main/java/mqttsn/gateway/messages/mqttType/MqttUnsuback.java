package mqttsn.gateway.messages.mqttType;


import mqttsn.gateway.messages.MqttMessage;

public class MqttUnsuback extends MqttMessage {

	//Mqtt UNSUBACK fields
	private int msgId;
	
	/**
	 * MqttUnsuback constructor.Sets the appropriate message type. 
	 */
	public MqttUnsuback() {
		msgType = MqttMessage.UNSUBACK;
	}
	
	/**
	 * MqttUnsuback constructor.Sets the appropriate message type and constructs 
	 * a Mqtt UNSUBACK message from a received byte array.
	 * @param data: The buffer that contains the UNSUBACK message.
	 */
	public MqttUnsuback(byte[] data) {
		msgType = MqttMessage.UNSUBACK;
		msgId = ((data[2] & 0xFF) << 8) + (data[3] & 0xFF);
	}
	
	/**
	 * Method to convert this message to a byte array for transmission.
	 * @return A byte array containing the UNSUBACK message as it should appear on the wire.
	 * (Don't needed in the GW)
	 */
	public byte[] toBytes() {
		int length = 4;
		byte[] data = new byte[length];
		data [0] = (byte)((msgType << 4) & 0xF0);
		data [1] = (byte)0x02;//add Remaining length fields
		data [2] = (byte)((msgId >> 8) & 0xFF);
		data [3] = (byte) (msgId & 0xFF);			
		return data;
	}

	public int getMsgId() {
		return msgId;
	}

	public void setMsgId(int msgId) {
		this.msgId = msgId;
	}
}