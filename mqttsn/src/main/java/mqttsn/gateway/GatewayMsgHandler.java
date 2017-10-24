package mqttsn.gateway;


import mqttsn.gateway.messages.ControlMessage;
import mqttsn.gateway.messages.Message;
import mqttsn.gateway.messages.MqttMessage;
import mqttsn.gateway.messages.MqttSNMessage;
import mqttsn.gateway.messages.mqttSNType.MqttSNAdvertise;
import mqttsn.gateway.messages.mqttSNType.MqttSNGWInfo;
import mqttsn.gateway.messages.mqttSNType.MqttSNPublish;
import mqttsn.gateway.messages.mqttSNType.MqttSNSearchGW;
import mqttsn.gateway.messages.mqttType.*;
import mqttsn.gateway.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringTokenizer;
import java.util.Vector;

public class GatewayMsgHandler extends MsgHandler {
    private static final Logger LOG = LoggerFactory.getLogger(GatewayMsgHandler.class);

    private GatewayAddress gatewayAddress = null;
    private Broker broker= null;
    private TimerService timer = null;
    private Dispatcher dispatcher;
    private long advPeriodCounter = 0;
    private long checkingCounter = 0;
    private TopicMappingTable topicIdMappingTable;
    private String clientId;

    private boolean connected;
    private Vector<ClientInterface> clientInterfacesVector;

    public GatewayMsgHandler(GatewayAddress addr) {
        this.gatewayAddress = addr;
    }

    @Override
    public void initialize() {
        broker = new Broker(this.gatewayAddress);
        timer = TimerService.getInstance();
        dispatcher = Dispatcher.getInstance();
        topicIdMappingTable = new TopicMappingTable();
        topicIdMappingTable.initialize();
        clientId = "Gateway_" + GWParameters.getGwId();

        LOG.info("Establishing TCP/IP connection with "+ GWParameters.getBrokerURL());

        //开启一个新的TCP连接
        try {
            broker.initialize();
        } catch (MqttSNException e) {
           LOG.error("Failed to establish TCP/IP connection with " + GWParameters.getBrokerURL()+ ". Gateway cannot start.",e);
            System.exit(1);

        }
        LOG.info("TCP/IP connection established.");
    }

    @Override
    public void handleMqttSNMessage(MqttSNMessage receivedMsg) {
        switch(receivedMsg.getMsgType()){
            case MqttSNMessage.ADVERTISE:
                handleMqttSNAdvertise((MqttSNAdvertise) receivedMsg);
                break;

            case MqttSNMessage.SEARCHGW:
                handleMqttSNSearchGW((MqttSNSearchGW) receivedMsg);
                break;

            case MqttSNMessage.GWINFO:
                handleMqttSNGWInfo((MqttSNGWInfo) receivedMsg);
                break;

            case MqttSNMessage.CONNECT:
                break;

            case MqttSNMessage.CONNACK:
                break;

            case MqttSNMessage.WILLTOPICREQ:
                break;

            case MqttSNMessage.WILLTOPIC:
                break;

            case MqttSNMessage.WILLMSGREQ:
                break;

            case MqttSNMessage.WILLMSG:
                break;

            case MqttSNMessage.REGISTER:
                break;

            case MqttSNMessage.REGACK:
                break;

            case MqttSNMessage.PUBLISH:
                handleMqttSNPublish((MqttSNPublish) receivedMsg);
                break;

            case MqttSNMessage.PUBACK:
                break;

            case MqttSNMessage.PUBCOMP:
                break;

            case MqttSNMessage.PUBREC:
                break;

            case MqttSNMessage.PUBREL:
                break;

            case MqttSNMessage.SUBSCRIBE:
                break;

            case MqttSNMessage.SUBACK:
                break;

            case MqttSNMessage.UNSUBSCRIBE:
                break;

            case MqttSNMessage.UNSUBACK:
                break;

            case MqttSNMessage.PINGREQ:
                break;

            case MqttSNMessage.PINGRESP:
                break;

            case MqttSNMessage.DISCONNECT:
                break;

            case MqttSNMessage.WILLTOPICUPD:
                break;

            case MqttSNMessage.WILLTOPICRESP:
                break;

            case MqttSNMessage.WILLMSGUPD:
                break;

            case MqttSNMessage.WILLMSGRESP:
                break;

            default:
                LOG.warn("MqttSN message of unknown type \"" + receivedMsg.getMsgType()+"\" received.");
                break;
        }

    }

    private void handleMqttSNAdvertise(MqttSNAdvertise receivedMsg) {
        LOG.info("MqttSN ADVERTISE message received.");
    }

    private void handleMqttSNSearchGW(MqttSNSearchGW receivedMsg) {
       MqttSNGWInfo msg = new MqttSNGWInfo();
        msg.setGwId(GWParameters.getGwId());

        //get the broadcast radius
        byte radius = (byte)receivedMsg.getRadius();
        Vector<?> interfaces = GWParameters.getClientInterfaces();
        for(int i = 0; i < interfaces.size(); i++){
            ClientInterface inter = (ClientInterface) interfaces.get(i);
            inter.broadcastMsg(radius, msg);
        }
    }

    private void handleMqttSNGWInfo(MqttSNGWInfo receivedMsg) {
        LOG.info("MqttSN GWINFO message received.");

    }

    private void handleMqttSNPublish(MqttSNPublish receivedMsg) {
        if(receivedMsg.getTopicIdType() == MqttSNMessage.NORMAL_TOPIC_ID)
            LOG.info("MqttSN PUBLISH message with \"QoS\" = \""+receivedMsg.getQos()+"\" and \"TopicId\" = \""+receivedMsg.getTopicId()+"\" received.");
        else if (receivedMsg.getTopicIdType() == MqttSNMessage.PREDIFINED_TOPIC_ID)
            LOG.info("MqttSN PUBLISH message with \"QoS\" = \""+receivedMsg.getQos()+"\" and \"TopicId\" = \""+receivedMsg.getTopicId()+"\" (predefined topic Id) received.");
        else
            LOG.info("MqttSN PUBLISH message with \"QoS\" = \""+receivedMsg.getQos()+"\" and \"TopicId\" = \""+receivedMsg.getShortTopicName()+"\" (short topic name) received.");

        MqttPublish publish = new MqttPublish();

        switch(receivedMsg.getTopicIdType()){
            case MqttSNMessage.NORMAL_TOPIC_ID:
                LOG.warn("Topic Id type "+ receivedMsg.getTopicIdType()+" is invalid. Publish with \"QoS\" = \"-1\" supports only predefined topis Ids (topic Id type = \"1\") or short topic names (topic Id type = \"2\").");
                return;
            case MqttSNMessage.SHORT_TOPIC_NAME:
                publish.setTopicName(receivedMsg.getShortTopicName());
                break;
            case MqttSNMessage.PREDIFINED_TOPIC_ID:
                if(receivedMsg.getTopicId() > GWParameters.getPredfTopicIdSize()){
                    LOG.warn("Predefined topicId (\"" + receivedMsg.getTopicId() + "\") of the received MqttSN PUBLISH message is out of the range of predefined topic Ids [1,"+GWParameters.getPredfTopicIdSize()+"]. The message cannot be processed.");
                    return;
                }

                String topicName = topicIdMappingTable.getTopicName(receivedMsg.getTopicId());
                if(topicName == null){
                    LOG.warn("Predefined topicId (\"" + receivedMsg.getTopicId() + "\") of the received MqttSN PUBLISH message does not exist. The message cannot be processed.");
                    return;
                }
                publish.setTopicName(topicName);
                break;

            default:
                LOG.warn("Unknown topicIdType (\"" + receivedMsg.getTopicIdType()+"\"). The received MqttSN PUBLISH message cannot be processed.");
                return;
        }

        //填充MQTT发布消息
        publish.setDup(false);
        publish.setQos(0);
        publish.setRetain(false);

        publish.setPayload(receivedMsg.getData());
        LOG.info("Sending Mqtt PUBLISH message with \"QoS\" = \""+publish.getQos()+"\" and \"TopicName\" = \""+publish.getTopicName()+ "\" to the broker.");
        try {
            broker.sendMsg(publish);
        } catch (MqttSNException e) {
            LOG.error("Failed sending Mqtt PUBLISH message to the broker.",e);
            connectionLost();
        }
    }
    @Override
    public void handleMqttMessage(MqttMessage receivedMsg) {
        switch(receivedMsg.getMsgType()){
            case MqttMessage.CONNECT:
                break;

            case MqttMessage.CONNACK:
                handleMqttConnack((MqttConnack) receivedMsg);
                break;

            case MqttMessage.PUBLISH:
                handleMqttPublish((MqttPublish) receivedMsg);
                break;

            case MqttMessage.PUBACK:
                handleMqttPuback((MqttPuback)receivedMsg);
                break;

            case MqttMessage.PUBREC:
                handleMqttPubRec((MqttPubRec) receivedMsg);
                break;

            case MqttMessage.PUBREL:
                handleMqttPubRel((MqttPubRel)receivedMsg);
                break;

            case MqttMessage.PUBCOMP:
                handleMqttPubComp((MqttPubComp) receivedMsg);
                break;

            case MqttMessage.SUBSCRIBE:
                break;

            case MqttMessage.SUBACK:
                handleMqttSuback((MqttSuback) receivedMsg);
                break;

            case MqttMessage.UNSUBSCRIBE:
                break;

            case MqttMessage.UNSUBACK:
                handleMqttUnsuback((MqttUnsuback) receivedMsg);
                break;

            case MqttMessage.PINGREQ:
                handleMqttPingReq((MqttPingReq) receivedMsg);
                break;

            case MqttMessage.PINGRESP:
                handleMqttPingResp((MqttPingResp) receivedMsg);
                break;

            case MqttMessage.DISCONNECT:
                break;

            default:
                LOG.warn("Mqtt message of unknown type \"" + receivedMsg.getMsgType()+"\" received.");
                break;
        }

    }

    private void handleMqttConnack(MqttConnack receivedMsg) {
        LOG.info("Mqtt CONNACK message received.");

        if (receivedMsg.getReturnCode() != MqttMessage.RETURN_CODE_CONNECTION_ACCEPTED){
           LOG.error("Return Code of Mqtt CONNACK message it is not Connection Accepted.Mqtt connection with the broker cannot be established. Gateway cannot start.");
            System.exit(1);
        }
        LOG.info("Mqtt connection established.");

        this.connected = true;

        LOG.info("Initializing all available Client interfaces...");

        clientInterfacesVector = new Vector<ClientInterface>();
        StringTokenizer st = new StringTokenizer(GWParameters.getClientIntString(),",");
        boolean init = false;
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            String clInte = token.substring(1, token.length()-1);
            ClientInterface inter = null;
            try {
                Class<?> cl = Class.forName(clInte);
                inter = (ClientInterface)cl.newInstance();
                inter.initialize();
                LOG.info(inter.getClass().getName()+ " initialized.");
                clientInterfacesVector.add(inter);
                init = true;
            }catch (Exception e) {
                LOG.error("Failed to instantiate "+clInte+".",e);
            }
        }

        if(!init){
           LOG.error("Failed to initialize at least one Client interface.Gateway cannot start.");
            System.exit(1);
        }

        GWParameters.setClientInterfacesVector(clientInterfacesVector);

        sendMqttPingReq();

        timer.register(gatewayAddress, ControlMessage.SEND_KEEP_ALIVE_MSG, GWParameters.getKeepAlivePeriod());
    }


    /**
     * @param receivedMsg
     */
    private void handleMqttPingResp(MqttPingResp receivedMsg) {
        // TODO Auto-generated method stub

    }

    /**
     * @param receivedMsg
     */
    private void handleMqttPingReq(MqttPingReq receivedMsg) {
      // TODO Auto-generated method stub

    }

    /**
     * @param receivedMsg
     */
    private void handleMqttUnsuback(MqttUnsuback receivedMsg) {
      // TODO Auto-generated method stub

    }

    /**
     * @param receivedMsg
     */
    private void handleMqttSuback(MqttSuback receivedMsg) {
      // TODO Auto-generated method stub

    }

    /**
     * @param receivedMsg
     */
    private void handleMqttPubComp(MqttPubComp receivedMsg) {
      // TODO Auto-generated method stub

    }

    /**
     * @param receivedMsg
     */
    private void handleMqttPubRel(MqttPubRel receivedMsg) {
      // TODO Auto-generated method stub

    }

    /**
     * @param receivedMsg
     */
    private void handleMqttPubRec(MqttPubRec receivedMsg) {
       // TODO Auto-generated method stub

    }

    /**
     * @param receivedMsg
     */
    private void handleMqttPuback(MqttPuback receivedMsg) {
       // TODO Auto-generated method stub

    }

    private void handleMqttPublish(MqttPublish receivedMsg) {
       // TODO Auto-generated method stub

    }

    @Override
    public void handleControlMessage(ControlMessage receivedMsg) {
        switch(receivedMsg.getMsgType()){
            case ControlMessage.CONNECTION_LOST:
                connectionLost();
                break;

            case ControlMessage.WAITING_WILLTOPIC_TIMEOUT:
                break;

            case ControlMessage.WAITING_WILLMSG_TIMEOUT:
                break;

            case ControlMessage.WAITING_REGACK_TIMEOUT:
                break;

            case ControlMessage.CHECK_INACTIVITY:
                break;

            case ControlMessage.SEND_KEEP_ALIVE_MSG:
                handleControlKeepAlive();
                break;

            case ControlMessage.SHUT_DOWN:
                shutDown();
                break;

            default:
                LOG.warn("Control message of unknown type " + receivedMsg.getMsgType()+"received.");
                break;
        }
    }

    private void connectionLost(){
       LOG.error("TCP/IP connection with the broker was lost.");

        if (this.connected){
            broker.disConnect();
            this.connected = false;

            ControlMessage controlMsg = new ControlMessage();
            controlMsg.setMsgType(ControlMessage.SHUT_DOWN);

            Message msg = new Message(null);
            msg.setType(Message.CONTROL_MSG);
            msg.setControlMessage(controlMsg);
            this.dispatcher.putMessage(msg);
        }else{
            LOG.error("Failed to establish Mqtt connection with the broker.Gateway cannot start.");
            System.exit(1);
        }
    }

    /**
     *
     */
    private void handleControlKeepAlive() {

        sendMqttPingReq();
        advPeriodCounter = advPeriodCounter + GWParameters.getKeepAlivePeriod();
        if (advPeriodCounter >= GWParameters.getAdvPeriod()){
            advPeriodCounter = 0;
        }

        checkingCounter = checkingCounter + GWParameters.getKeepAlivePeriod();
        if(checkingCounter >= GWParameters.getCkeckingPeriod ()){
            sendCheckInactivity();
            checkingCounter = 0;
        }
    }

    /**
     *
     */
    private void shutDown() {
        broker.setRunning(false);
        MqttDisconnect mqttDisconnect = new MqttDisconnect();
        try {
            broker.sendMsg(mqttDisconnect);
        } catch (MqttSNException e) {

        }
        broker.disConnect();
    }

    public void connect() {
        MqttConnect mqttConnect = new MqttConnect();
        mqttConnect.setProtocolName(GWParameters.getProtocolName());
        mqttConnect.setProtocolVersion (GWParameters.getProtocolVersion());
        mqttConnect.setWillRetain (GWParameters.isRetain());
        mqttConnect.setWillQoS (GWParameters.getWillQoS());
        mqttConnect.setWill (GWParameters.isWillFlag());
        mqttConnect.setCleanStart (GWParameters.isCleanSession());
        mqttConnect.setKeepAlive(GWParameters.getKeepAlivePeriod());
        mqttConnect.setClientId (clientId);
        mqttConnect.setWillTopic (GWParameters.getWillTopic());
        mqttConnect.setWillMessage (GWParameters.getWillMessage());

        LOG.info("Establishing MQTT connection with the broker...");

        try {
            broker.sendMsg(mqttConnect);
        } catch (MqttSNException e) {
           LOG.error("Failed to establish Mqtt connection with the broker. Gateway cannot start.",e);
            System.exit(1);
        }
    }

    private void sendMqttPingReq() {
        MqttPingReq pingreq = new MqttPingReq();

        try {
            broker.sendMsg(pingreq);
        } catch (MqttSNException e) {
            LOG.error("Failed sending MqttSN PINGREQ message to the broker.",e);
            connectionLost();
        }
    }

    private void sendCheckInactivity() {
        ControlMessage controlMsg = new ControlMessage();
        controlMsg.setMsgType(ControlMessage.CHECK_INACTIVITY);

        Message msg = new Message(null);

        msg.setType(Message.CONTROL_MSG);
        msg.setControlMessage(controlMsg);
        this.dispatcher.putMessage(msg);
    }
}
