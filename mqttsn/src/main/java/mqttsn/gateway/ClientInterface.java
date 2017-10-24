package mqttsn.gateway;

import mqttsn.gateway.messages.MqttSNMessage;
import mqttsn.gateway.utils.ClientAddress;

/**
 * Created by jungao on 2017/10/19.
 */
public interface ClientInterface {

    /**
     * 初始化方法
     * @throws MqttSNException
     */
    void initialize() throws MqttSNException;

    /**
     * 发送MQTT-SN消息
     * @param address
     * @param msg
     */
     void sendMsg(ClientAddress address, MqttSNMessage msg);

    /**
     * 广播消息
     * @param msg
     */
     void broadcastMsg(MqttSNMessage msg);

    /**
     *
     * @param radius
     * @param msg
     */
     void broadcastMsg(int radius, MqttSNMessage msg);
}
