package mqttsn.gateway;


import mqttsn.gateway.messages.ControlMessage;
import mqttsn.gateway.messages.MqttMessage;
import mqttsn.gateway.messages.MqttSNMessage;

public abstract class MsgHandler {

    /**
     * 初始化方法
     */
    public abstract void initialize();

    /**
     * MQTT-SN的消息
     * @param msg
     */
    public abstract void handleMqttSNMessage(MqttSNMessage msg);

    /**
     * MQTT的消息
     * @param msg
     */
    public abstract void handleMqttMessage(MqttMessage msg);

    /**
     * 消息控制体内容，当消息发生异常是调用
     * @param msg
     */
    public abstract void handleControlMessage(ControlMessage msg);
}
