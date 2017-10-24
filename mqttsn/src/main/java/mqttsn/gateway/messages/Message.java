package mqttsn.gateway.messages;

import mqttsn.gateway.ClientInterface;
import mqttsn.gateway.utils.Address;

public class Message {

    public static final int MQTTSN_MSG = 1;
    public static final int MQTT_MSG = 2;
    public static final int CONTROL_MSG = 3;


    private final Address address;
    private int type;

    private MqttSNMessage mqttSNMessage = null;
    private MqttMessage mqttMessage = null;
    private ControlMessage controlMessage = null;

    private ClientInterface clientInterface = null;

    public Message(Address address) {
        this.address = address;
    }

    public Address getAddress() {
        return address;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public MqttSNMessage getMqttSNMessage() {
        return mqttSNMessage;
    }

    public void setMqttSNMessage(MqttSNMessage mqttsMessage) {
        this.mqttSNMessage = mqttsMessage;
    }

    public MqttMessage getMqttMessage() {
        return mqttMessage;
    }

    public void setMqttMessage(MqttMessage mqttMessage) {
        this.mqttMessage = mqttMessage;
    }

    public ControlMessage getControlMessage() {
        return controlMessage;
    }

    public void setControlMessage(ControlMessage controlMessage) {
        this.controlMessage = controlMessage;
    }

    public ClientInterface getClientInterface() {
        return clientInterface;
    }

    public void setClientInterface(ClientInterface clientInterface) {
        this.clientInterface = clientInterface;
    };
}
