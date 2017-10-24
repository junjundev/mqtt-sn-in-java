package mqttsn.gateway.messages.mqttType;

import mqttsn.gateway.messages.MqttMessage;

/**
 * 处理一个mqtt connack 的消息
 *
 */
public class MqttConnack extends MqttMessage {

	//Mqtt CONNACK 字段
	private boolean	topicNameCompression = false; //not used
	private int returnCode;

	/**
	 * 设置消息类型
	 */
	public MqttConnack() {
		msgType = MqttMessage.CONNACK;
	}

	/**
	 * 从接收到的字节数组构造connack MQTT消息。
	 */
	public MqttConnack(byte[] data) {
		msgType = MqttMessage.CONNACK;
		returnCode = (data[3] & 0xFF);
	}

	/**
	 * 将此消息转换为字节数组进行传输。
	 */
	public byte[] toBytes() {
		int length = 4;
		byte[] data = new byte[length];
		data [0] = (byte)((msgType << 4) & 0xF0);
		data [1] = (byte)0x02;
		data [2] = ((topicNameCompression) ? (byte)0x01 : (byte)0x00);
		data [3] = (byte) returnCode;
		return data;
	}


	public int getReturnCode() {
		return returnCode;
	}

	public void setReturnCode(int returnCode) {
		this.returnCode = returnCode;
	}
}