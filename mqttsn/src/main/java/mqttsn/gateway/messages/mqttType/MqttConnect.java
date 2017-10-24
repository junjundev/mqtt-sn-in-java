
package mqttsn.gateway.messages.mqttType;

import mqttsn.gateway.messages.MqttMessage;
import mqttsn.gateway.utils.Utils;

public class MqttConnect extends MqttMessage {

	//connect 字段
	private String protocolName;
	private int protocolVersion;
	private boolean cleanStart;
	private boolean topicNameCompression = false;
	private int keepAlive;
	private boolean will;
	private int willQoS;
	private boolean willRetain;
	private String willTopic;
	private String willMessage;
	private String clientId;

	/**
	 * 设置消息类型
	 */
	public MqttConnect() {
		msgType = MqttMessage.CONNECT;
	}

	public MqttConnect(byte[] data) {}


	/**
	 * 把接收到的消息转换为字节数组为了进行传输
	 * @return A byte array
	 */
	public byte[] toBytes() {
		int pos = 0; 
		byte[] data = new byte[MAX_CLIENT_ID_LENGTH + 19]; 
		data[pos++] = (byte)((msgType << 4) & 0xF0); 
		byte[] bytestring = Utils.StringToUTF(protocolName);
		System.arraycopy(bytestring, 0, data, pos, bytestring.length);
		pos += bytestring.length;
		data[pos++] = (byte) protocolVersion;

		byte compSub = ((topicNameCompression) ? (byte) 0x01 : (byte)0x00); // bit 0
		byte clean = ((cleanStart) ? (byte) 0x02 : (byte) 0x00); // bit 1
		byte theWill = (will) ? (byte) (((willRetain) ? 0x20 : 0x00) | // bit 5
				(byte) ((willQoS & 0x03) << 3) | // bit 4 & 3
				0x04 // bit 2
				) : (byte) 0x00;
		data[pos++] = (byte) (compSub | clean | theWill); // combine the bits
		/* KeepAlive field */
		data[pos++] = (byte) (keepAlive / 256); // MSB
		data[pos++] = (byte) (keepAlive % 256); // LSB
		// Client Id
		bytestring = Utils.StringToUTF(clientId);
		System.arraycopy(bytestring, 0, data, pos, bytestring.length);
		pos += bytestring.length;
		// Check if we want a will
		if (will) {
			// Add 'Will' topic
			byte[] topicbytes = Utils.StringToUTF(willTopic);
			// Add 'Will' data
			byte[] msgbytes = Utils.StringToUTF(willMessage);
			data = Utils.concatArray(Utils.concatArray(data,
					0, pos, topicbytes, 0, topicbytes.length), msgbytes);
			pos += topicbytes.length + msgbytes.length;
		}
		data = Utils.SliceByteArray(data, 0, pos);
		data = encodeMsgLength(data); // add Remaining length field
		return data;
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
	public boolean isCleanStart() {
		return cleanStart;
	}
	public void setCleanStart(boolean cleanStart) {
		this.cleanStart = cleanStart;
	}
	public int getKeepAlive() {
		return keepAlive;
	}
	public void setKeepAlive(int keepAlive) {
		this.keepAlive = keepAlive;
	}
	public boolean isWill() {
		return will;
	}
	public void setWill(boolean will) {
		this.will = will;
	}
	public int getWillQoS() {
		return willQoS;
	}
	public void setWillQoS(int willQoS) {
		this.willQoS = willQoS;
	}
	public boolean isWillRetain() {
		return willRetain;
	}
	public void setWillRetain(boolean willRetain) {
		this.willRetain = willRetain;
	}
	public String getWillTopic() {
		return willTopic;
	}
	public void setWillTopic(String willTopic) {
		this.willTopic = willTopic;
	}
	public String getWillMessage() {
		return willMessage;
	}
	public void setWillMessage(String willMessage) {
		this.willMessage = willMessage;
	}
	public String getClientId() {
		return clientId;
	}

	public void setClientId(String aClientId) {
		clientId = aClientId;
	}

}
