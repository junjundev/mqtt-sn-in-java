package mqttsn.gateway;

import mqttsn.gateway.messages.MqttMessage;

/**
 * Created by jungao on 2017/10/19.
 * 接口网关发送消息到MQTTBroker，反之亦然
 */

public interface BrokerInterface {

    /**
    * 初始化BrokerInterface接口
    */
    void initialize() throws MqttSNException;

    /**
     * 从MQTTBroker中读取消息
     */
    void readMsg();

    /**
     * 发送消息到MQTTBroker中
     */
    void sendMsg(MqttMessage message) throws MqttSNException;

    /**
     * 关闭连接
     */
    void disConnect();
}
