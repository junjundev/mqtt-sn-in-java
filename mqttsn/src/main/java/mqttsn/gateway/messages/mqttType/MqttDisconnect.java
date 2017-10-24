package mqttsn.gateway.messages.mqttType;


import mqttsn.gateway.messages.MqttMessage;

/**
 * This object represents a Mqtt DISCONNECT message.
 * 
 *
 */
public class MqttDisconnect extends MqttMessage {

	/**
	 * MqttDisconnect constructor.Sets the appropriate message type. 
	 */
	public MqttDisconnect() {
		msgType = MqttMessage.DISCONNECT;
	}

	/**
	 * This method is not needed in the GW
	 */
	public MqttDisconnect(byte[] data) {}

	/**
	 * Method to convert this message to byte array for transmission.
	 * @return A byte array containing the DISCONNECT message as it should appear on the wire.
	 */
	public byte[] toBytes() {
		int length = 2;
		byte[] data = new byte[length];
		data [0] = (byte)((msgType << 4) & 0xF0);
		data [1] = (byte)0x00;//add Remaining length field
		return data;
	}
}
