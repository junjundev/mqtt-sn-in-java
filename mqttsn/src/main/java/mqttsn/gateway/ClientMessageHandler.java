package mqttsn.gateway;

import mqttsn.gateway.messages.ControlMessage;
import mqttsn.gateway.messages.Message;
import mqttsn.gateway.messages.MqttMessage;
import mqttsn.gateway.messages.MqttSNMessage;
import mqttsn.gateway.messages.mqttSNType.*;
import mqttsn.gateway.messages.mqttType.*;
import mqttsn.gateway.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

 * 该类是协议转换的核心功能
 * 每一个客户端都是该对象的一个实例，对于每一个客户端指定的每种消息类型（MQTT，MQTT-SN或CONTROL）都是由该对象处理
 */
public class ClientMessageHandler extends MsgHandler{
    private static final Logger LOG = LoggerFactory.getLogger(ClientMessageHandler.class);

    //区分该对象是根据客户端的唯一地址
    private ClientAddress clientAddress = null;

    //客户端的ID
    private String clientId = "...";

    //在发送一个MQTT-SN的消息到客户端时响应
    private ClientInterface clientInterface = null;
    private Broker broker = null;

    //用于超时服务
    private TimerService timer = null;

    //将topicId和topicName映射
    private TopicMappingTable topicIdMappingTable = null;

    private Dispatcher dispatcher = null;

    //标记客户端的状态
    private ClientState client = null;

    //标记网关的状态
    private GatewayState gateway = null;

    //超时时间
    private long timeout;

    //响应客户端连接信息
    private MqttSNConnect mqttSNConnect = null;
    private MqttSNWillTopic mqttSNWillTopic = null;

    //响应客户端的订阅/取消订阅信息
    private MqttSNSubscribe mqttSNSubscribe = null;
    private MqttSNUnsubscribe mqttSNUnsubscribe = null;

    //在网关发起的注册过程中存储信息的消息
    private MqttSNRegister mqttSNRegister = null;

    //收到MQTT发布消息
    private MqttPublish mqttPublish = null;

    //收到MQTT-SN发布消息
    private MqttSNPublish mqttSNPublish = null;

    private int msgId;
    private int topicId;

    public ClientMessageHandler(ClientAddress addr) {
        this.clientAddress = addr;
    }

    @Override
    public void initialize() {
        broker = new Broker(this.clientAddress);
        broker.setClientId(clientId);
        timer = TimerService.getInstance();
        dispatcher = Dispatcher.getInstance();
        topicIdMappingTable = new TopicMappingTable();
        topicIdMappingTable.initialize();
        timeout = 0;
        client = new ClientState();
        gateway = new GatewayState();
        msgId = 1;
        topicId = GWParameters.getPredfTopicIdSize()+1;

    }

    @Override
    public void handleMqttSNMessage(MqttSNMessage msg) {
        //更新超时时间
        timeout = System.currentTimeMillis() + GWParameters.getHandlerTimeout() * 1000;

        switch (msg.getMsgType()) {
            case MqttSNMessage.ADVERTISE:  //从客户端不会接受到这样的消息
                break;
            case MqttSNMessage.SEARCHGW:
                handleMqttSNSearchGW((MqttSNSearchGW) msg);
                break;
            case MqttSNMessage.GWINFO:
                break;
            case MqttSNMessage.CONNECT:
                handleMqttSNConnect((MqttSNConnect) msg);
                break;
            case MqttSNMessage.CONNACK:
                break;
            case MqttSNMessage.WILLTOPICREQ:
                break;
            case MqttSNMessage.WILLTOPIC:
                handleMqttSNWillTopic((MqttSNWillTopic) msg);
                break;
            case MqttSNMessage.WILLMSGREQ:
                break;
            case MqttSNMessage.WILLMSG:
                hangdleMqttSNMessage((MqttSNWillMsg) msg);
                break;
            case MqttSNMessage.REGISTER:
                handleMqttSNRegister((MqttSNRegister)msg);
                break;

            case MqttSNMessage.REGACK:
                handleMqttSNRegack((MqttSNRegack) msg);
                break;

            case MqttSNMessage.PUBLISH:
                handleMqttSNPublish((MqttSNPublish) msg);
                break;

            case MqttSNMessage.PUBACK:
                handleMqttSNPuback((MqttSNPuback) msg);
                break;

            case MqttSNMessage.PUBCOMP:
                handleMqttSNPubComp((MqttSNPubComp) msg);
                break;

            case MqttSNMessage.PUBREC:
                handleMqttSNPubRec((MqttSNPubRec) msg);
                break;

            case MqttSNMessage.PUBREL:
                handleMqttSNPubRel((MqttSNPubRel) msg);
                break;

            case MqttSNMessage.SUBSCRIBE:
                handleMqttSNSubscribe((MqttSNSubscribe) msg);
                break;

            case MqttSNMessage.SUBACK:
                break;

            case MqttSNMessage.UNSUBSCRIBE:
                handleMqttSNUnsubscribe((MqttSNUnsubscribe) msg);
                break;

            case MqttSNMessage.UNSUBACK:
                break;

            case MqttSNMessage.PINGREQ:
                handleMqttSNPingReq((MqttSNPingReq) msg);
                break;

            case MqttSNMessage.PINGRESP:
                handleMqttSNPingResp((MqttSNPingResp) msg);
                break;

            case MqttSNMessage.DISCONNECT:
                handleMqttSNDisconnect((MqttSNDisconnect) msg);
                break;

            case MqttSNMessage.WILLTOPICUPD:
                handleMqttSNWillTopicUpd((MqttSNWillTopicUpd) msg);
                break;

            case MqttSNMessage.WILLTOPICRESP:
                break;

            case MqttSNMessage.WILLMSGUPD:
                handleMqttSNWillMsgUpd((MqttSNWillMsgUpd) msg);
                break;

            case MqttSNMessage.WILLMSGRESP:
                break;

            default:
                LOG.warn("ClientMsgHandler ["+ Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN message of unknown type \"" + msg.getMsgType()+"\" received.");
                break;
        }

    }

    @Override
    public void handleMqttMessage(MqttMessage msg) {

        timeout = System.currentTimeMillis() + GWParameters.getHandlerTimeout()*1000;
        switch(msg.getMsgType()){
            case MqttMessage.CONNECT:
                break;

            case MqttMessage.CONNACK:
                handleMqttConnack((MqttConnack) msg);
                break;

            case MqttMessage.PUBLISH:
                handleMqttPublish((MqttPublish) msg);
                break;

            case MqttMessage.PUBACK:
                handleMqttPuback((MqttPuback)msg);
                break;

            case MqttMessage.PUBREC:
                handleMqttPubRec((MqttPubRec) msg);
                break;

            case MqttMessage.PUBREL:
                handleMqttPubRel((MqttPubRel)msg);
                break;

            case MqttMessage.PUBCOMP:
                handleMqttPubComp((MqttPubComp) msg);
                break;

            case MqttMessage.SUBSCRIBE:
                break;

            case MqttMessage.SUBACK:
                handleMqttSuback((MqttSuback) msg);
                break;

            case MqttMessage.UNSUBSCRIBE:
                break;

            case MqttMessage.UNSUBACK:
                handleMqttUnsuback((MqttUnsuback) msg);
                break;

            case MqttMessage.PINGREQ:
                handleMqttPingReq((MqttPingReq) msg);
                break;

            case MqttMessage.PINGRESP:
                handleMqttPingResp((MqttPingResp) msg);
                break;

            case MqttMessage.DISCONNECT:
                break;

            default:
               LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Mqtt message of unknown type \"" + msg.getMsgType()+"\" received.");
                break;
        }
    }

    @Override
    public void handleControlMessage(ControlMessage msg) {
        switch(msg.getMsgType()){
            case ControlMessage.CONNECTION_LOST:
                connectionLost();
                break;

            case ControlMessage.WAITING_WILLTOPIC_TIMEOUT:
                handleWaitingWillTopicTimeout();
                break;

            case ControlMessage.WAITING_WILLMSG_TIMEOUT:
                handleWaitingWillMsgTimeout();
                break;

            case ControlMessage.WAITING_REGACK_TIMEOUT:
                handleWaitingRegackTimeout();
                break;

            case ControlMessage.CHECK_INACTIVITY:
                handleCheckInactivity();
                break;

            case ControlMessage.SEND_KEEP_ALIVE_MSG:
                break;

            case ControlMessage.SHUT_DOWN:
                shutDown();
                break;

            default:
               LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Control message of unknown type \"" + msg.getMsgType()+"\" received.");
                break;
        }
    }

    /**
     * 处理SearchGW的的请求消息
     * @param receiveMsg
     */
    private void handleMqttSNSearchGW(MqttSNSearchGW receiveMsg) {
        LOG.info("开始处理SearchGateway...");
        GatewayAddress gatewayAddress = GWParameters.getGatewayAddress();
        Message message = new Message(gatewayAddress);
        message.setType(Message.MQTTSN_MSG);
        message.setMqttSNMessage(receiveMsg);
        dispatcher.putMessage(message);
    }

    /**
     * 处理MQTT-SN连接信息
     * @param receiveMsg
     */
    private void handleMqttSNConnect(MqttSNConnect receiveMsg) {
        LOG.info("ClientMsgHandler["+ Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is already connected. MqttSN CONNECT message with \"Will\" = \"" +receiveMsg.isWill()+"\" and \"CleanSession\" = \"" +receiveMsg.isCleanSession()+"\" received.");
        this.clientId = receiveMsg.getClientId();
        broker.setClientId(clientId);

        //如果客户端已经连接，则返回一个connack信息
        if (client.isConnected()) {
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is already connected. MqttSN CONNACK message will be send to the client.");
            MqttSNConnack connack = new MqttSNConnack();
            connack.setReturnCode(MqttSNMessage.RETURN_CODE_ACCEPTED);
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN CONNACK message to the client.");
            clientInterface.sendMsg(this.clientAddress,connack);
            return;
        }

        //如果网关已经在与客户机正在建立连接，请删除消息
        if (gateway.isEstablishingConnection()) {
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is already establishing a connection. The received MqttSN CONNECT message cannot be processed.");
            return;
        }
        if (!receiveMsg.isWill()) {
            MqttConnect mqttConnect = new MqttConnect();
            mqttConnect.setProtocolName(receiveMsg.getProtocolName());
            mqttConnect.setProtocolVersion(receiveMsg.getProtocolVersion());
            mqttConnect.setWill(receiveMsg.isWill());
            mqttConnect.setCleanStart(receiveMsg.isCleanSession());
            mqttConnect.setKeepAlive(receiveMsg.getDuration());
            mqttConnect.setClientId(receiveMsg.getClientId());

            //打开一个连接
            try {
                broker.initialize();
            } catch (MqttSNException e) {
                LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - An error occurred while TCP/IP connection setup with the broker.",e);
                return;
            }
            //发送一个connect 消息到broker
            try {
                broker.sendMsg(mqttConnect);
            } catch (MqttSNException e) {
                LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Failed sending Mqtt CONNECT message to the broker.",e);
                return;
            }
            //标记客户端状态为connected
            client.setConnected();
            return;
        }
        this.mqttSNConnect = receiveMsg;
        MqttSNWillTopicReq willTopicReq = new MqttSNWillTopicReq();
        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN WILLTOPICREQ message to the client.");
        clientInterface.sendMsg(this.clientAddress,willTopicReq);
        gateway.setWaitingWillTopic();
        gateway.increaseTriesSendingWillTopicReq();
        timer.register(this.clientAddress,ControlMessage.WAITING_WILLTOPIC_TIMEOUT,GWParameters.getWaitingTime());
    }

    /**
     * 处理MQTT-SN的遗嘱topic消息
     * @param receiveMsg
     */
    private void handleMqttSNWillTopic(MqttSNWillTopic receiveMsg) {
        if (gateway.isWaitingWillTopic()) {
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Gateway is not waiting a MqttSN WILLTOPIC message from the client. The received message cannot be processed.");
            return;
        }
        gateway.resetWaitingWillTopic();
        gateway.resetTriesSendingWillTopicReq();
        timer.unregister(this.clientAddress,ControlMessage.WAITING_WILLTOPIC_TIMEOUT);
        this.mqttSNWillTopic = receiveMsg;
        MqttSNWillMsgReq willMsgReq = new MqttSNWillMsgReq();
        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN WILLMSGREQ message to the client.");
        clientInterface.sendMsg(this.clientAddress,willMsgReq);
        gateway.setWaitingWillMsg();
        gateway.increaseTriesSendingWillMsgReq();
        timer.register(this.clientAddress,ControlMessage.WAITING_WILLMSG_TIMEOUT,GWParameters.getWaitingTime());
    }

    /**
     *处理MQTT-SN遗嘱信息
     */
    private void hangdleMqttSNMessage(MqttSNWillMsg receiveMsg) {
        if (!gateway.isWaitingWillMsg()) {
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Gateway is not waiting a MqttSN WILLMSG message from the client.The received message cannot be processed.");
            return;
        }
        gateway.resetWaitingWillMsg();
        gateway.resetTriesSendingWillMsgReq();
        timer.unregister(this.clientAddress,ControlMessage.WAITING_WILLMSG_TIMEOUT);
        if (this.mqttSNConnect == null) {
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - The stored MqttSN CONNECT message is null. The received MqttSN WILLMSG message cannot be processed.");
            this.mqttSNWillTopic = null;
            return;
        }
        if (this.mqttSNWillTopic == null) {
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - The stored MqttSN WILLTOPIC message is null. The received MqttSN WILLMSG message cannot be processed.");
            this.mqttSNConnect = null;
            return;
        }
        MqttConnect mqttConnect = new MqttConnect();
        mqttConnect.setClientId(this.mqttSNConnect.getClientId());
        mqttConnect.setKeepAlive(this.mqttSNConnect.getDuration());
        mqttConnect.setCleanStart(this.mqttSNConnect.isCleanSession());
        mqttConnect.setProtocolName(this.mqttSNConnect.getProtocolName());
        mqttConnect.setProtocolVersion(this.mqttSNConnect.getProtocolVersion());
        mqttConnect.setWillRetain (this.mqttSNWillTopic.isRetain());
        mqttConnect.setWillQoS (this.mqttSNWillTopic.getQos());
        mqttConnect.setWill (this.mqttSNConnect.isWill());
        mqttConnect.setWillTopic (this.mqttSNWillTopic.getWillTopic());
        mqttConnect.setWillMessage (receiveMsg.getWillMsg());

        try {
            broker.initialize();
        } catch (MqttSNException e) {
            LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - An error occurred while TCP/IP connection setup with the broker.",e);
            return;
        }
        try {
            broker.sendMsg(mqttConnect);
        } catch (MqttSNException e) {
            LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Failed sending Mqtt CONNECT message to the broker.",e);
            return;
        }
        client.setConnected();
        this.mqttSNConnect = null;
        this.mqttSNWillTopic = null;
    }

    /**
     * 处理MQTT-SN注册消息操作
     * @param receive
     */
    private void handleMqttSNRegister(MqttSNRegister receive) {
        if (!client.isConnected()) {
            sendClientDisconnect();
            return;
        }

        int topicId = topicIdMappingTable.getTopicId(receive.getTopicName());
        if (topicId == 0) {
            topicId = getNewTopicId();
            topicIdMappingTable.assignTopicId(topicId,receive.getTopicName());
        }
        MqttSNRegack mqttSNRegack = new MqttSNRegack();
        mqttSNRegack.setTopicId(topicId);
        mqttSNRegack.setMsgId(receive.getMsgId());
        mqttSNRegack.setReturnCode(MqttSNMessage.RETURN_CODE_ACCEPTED);

        clientInterface.sendMsg(this.clientAddress,receive);
    }

    /**
     * MQTT-SN Register Ack
     * @param receive
     */
    private void handleMqttSNRegack(MqttSNRegack receive) {
        if (!client.isConnected()) {
            sendClientDisconnect();
            return;
        }
        if (!gateway.isWaitingRegack()) {
            return;
        }

        if (this.mqttSNRegister == null) {
            gateway.resetWaitingRegack();
            gateway.resetTriesSendingRegister();
            timer.unregister(this.clientAddress,ControlMessage.WAITING_REGACK_TIMEOUT);
            this.mqttPublish = null;
            return;
        }

        if (this.mqttPublish == null) {
            gateway.resetWaitingRegack();
            gateway.resetTriesSendingRegister();
            timer.unregister(this.clientAddress,ControlMessage.WAITING_REGACK_TIMEOUT);
            this.mqttSNPublish = null;
            return;
        }

        if (receive.getMsgId() != this.mqttSNRegister.getMsgId()) {
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MsgId (\""+receive.getMsgId()+"\") of the received MqttSN REGACK message does not match the MsgId (\""+this.mqttSNRegister.getMsgId()+"\") of the stored MqttSN REGISTER message. The message cannot be processed.");
            return;
        }

        topicIdMappingTable.assignTopicId(receive.getTopicId(),this.mqttPublish.getTopicName());
        MqttSNPublish publish = new MqttSNPublish();
        publish.setDup(mqttPublish.isDup());
        publish.setQos(mqttPublish.getQos());
        publish.setRetain(mqttPublish.isRetain());
        publish.setTopicIdType(MqttSNMessage.NORMAL_TOPIC_ID);
        publish.setTopicId(receive.getTopicId());
        publish.setMsgId(mqttPublish.getMsgId());
        publish.setData(mqttPublish.getPayload());
        clientInterface.sendMsg(this.clientAddress, publish);

        gateway.resetWaitingRegack();
        gateway.resetTriesSendingRegister();
        timer.unregister(this.clientAddress, ControlMessage.WAITING_REGACK_TIMEOUT);
        this.mqttSNRegister = null;
        this.mqttPublish = null;
    }

    /**
     * 处理publish的消息
     * @param receive
     */
    private void handleMqttSNPublish(MqttSNPublish receive) {

        if(receive.getTopicIdType() == MqttSNMessage.NORMAL_TOPIC_ID) {
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN PUBLISH message with \"QoS\" = \""+receive.getQos()+"\" and \"TopicId\" = \""+receive.getTopicId()+"\" received.");
        } else if (receive.getTopicIdType() == MqttSNMessage.PREDIFINED_TOPIC_ID) {
            LOG.info("ClientMsgHandler [" + Utils.hexString(this.clientAddress.getAddress()) + "]/[" + clientId + "] - MqttSN PUBLISH message with \"QoS\" = \"" + receive.getQos() + "\" and \"TopicId\" = \"" + receive.getTopicId() + "\" (predefined topid Id) received.");
        } else if (receive.getTopicIdType() == MqttSNMessage.SHORT_TOPIC_NAME) {
            LOG.info("ClientMsgHandler [" + Utils.hexString(this.clientAddress.getAddress()) + "]/[" + clientId + "] - MqttSN PUBLISH message with \"QoS\" = \"" + receive.getQos() + "\" and \"TopicId\" = \"" + receive.getShortTopicName() + "\" (short topic name) received.");
        } else {
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN PUBLISH message with unknown topicIdType (\"" + receive.getTopicIdType()+"\") received. The message cannot be processed.");
            return;
        }
        if (receive.getQos() == -1) {
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - The received MqttSN PUBLISH message with \"QoS\" = \"-1\" will be handled by GatewayMsgHandler.");
            Message message = new Message(GWParameters.getGatewayAddress());

            message.setType(Message.MQTTSN_MSG);
            message.setMqttSNMessage(receive);
            dispatcher.putMessage(message);
            return;
        }

        if (!client.isConnected()) {
            sendClientDisconnect();
            return;
        }

        if (gateway.isWaitingPuback() && receive.getQos() == 1) {
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is already in a publish procedure with \"QoS\" = \"1\". The received MqttSN PUBLISH message with \"QoS\" = \""+receive.getQos()+"\" cannot be processed.");
            return;
        }
        MqttPublish publish = new MqttPublish();
        switch (receive.getTopicIdType()) {
            case MqttSNMessage.NORMAL_TOPIC_ID:
                if (receive.getTopicId() <= GWParameters.getPredfTopicIdSize()) {
                    LOG.info( "ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - TopicId (\"" + receive.getTopicId() + "\") of the received Mqtts PUBLISH message is in the range of predefined topic Ids [1,"+GWParameters.getPredfTopicIdSize()+"]. The message cannot be processed. MqttSN PUBACK with rejection reason will be sent to the client.");
                    MqttSNPuback puback = new MqttSNPuback();
                    puback.setTopicId(receive.getTopicId());
                    puback.setMsgId(receive.getMsgId());
                    puback.setReturnCode(MqttSNMessage.RETURN_CODE_INVALID_TOPIC_ID);

                    clientInterface.sendMsg(this.clientAddress,puback);
                    return;

                }
                String topicName = topicIdMappingTable.getTopicName(receive.getTopicId());
                if (topicName == null) {
                    LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - TopicId (\"" + receive.getTopicId() + "\") of the received MqttSN PUBLISH message does not exist. The message cannot be processed. MqttSN PUBACK with rejection reason will be sent to the client.");
                    MqttSNPuback puback = new MqttSNPuback();
                     puback.setMsgId(receive.getMsgId());
                     puback.setTopicId(receive.getTopicId());
                     puback.setReturnCode(MqttSNMessage.RETURN_CODE_INVALID_TOPIC_ID);
                     clientInterface.sendMsg(this.clientAddress,puback);
                     return;
                }
                publish.setTopicName(topicName);
                break;
            case MqttSNMessage.SHORT_TOPIC_NAME:
                publish.setTopicName(receive.getShortTopicName());
                break;
            case MqttSNMessage.PREDIFINED_TOPIC_ID:
                    if (receive.getTopicId() > GWParameters.getPredfTopicIdSize()) {
                        MqttSNPuback puback = new MqttSNPuback();
                        puback.setMsgId(receive.getMsgId());
                        puback.setTopicId(receive.getTopicId());
                        puback.setReturnCode(MqttSNMessage.RETURN_CODE_INVALID_TOPIC_ID);
                        clientInterface.sendMsg(this.clientAddress,puback);
                        return;
                    }
                 topicName = topicIdMappingTable.getTopicName(receive.getTopicId());
                    if (topicName == null) {
                        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Predefined topicId (\"" + receive.getTopicId() + "\") of the received MqttSN PUBLISH message does not exist. The message cannot be processed. MqttSN PUBACK with rejection reason will be sent to the client.");
                        MqttSNPuback puback = new MqttSNPuback();
                        puback.setMsgId(receive.getMsgId());
                        puback.setTopicId(receive.getTopicId());
                        puback.setReturnCode(MqttSNMessage.RETURN_CODE_INVALID_TOPIC_ID);
                        clientInterface.sendMsg(this.clientAddress,puback);
                        return;
                    }
                publish.setTopicName(topicName);
                break;
            default:
                    return;
        }
        publish.setDup(receive.isDup());
        publish.setQos(receive.getQos());
        publish.setRetain(receive.isRetain());
        publish.setMsgId(receive.getMsgId());
        publish.setPayload(receive.getData());

        try {
            broker.sendMsg(publish);
        } catch (MqttSNException e) {
            e.printStackTrace();
            LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Failed sending Mqtt PUBLISH message to the broker.");
            connectionLost();
            return;
        }
        if (receive.getQos() == 1) {
            gateway.setWaitingPuback();
            this.mqttSNPublish = receive;
        }
    }

    /**
     * 处理puback消息
     * @param receive
     */
    private void handleMqttSNPuback(MqttSNPuback receive) {
        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN PUBACK message received.");
        if (!client.isConnected()) {
            sendClientDisconnect();
            return;
        }
        if (receive.getReturnCode() == MqttSNMessage.RETURN_CODE_INVALID_TOPIC_ID) {
            topicIdMappingTable.removeTopicId(receive.getTopicId());
            return;
        }
        MqttPuback puback = new MqttPuback();
        puback.setMsgId(receive.getMsgId());

        try {
            broker.sendMsg(puback);
        } catch (MqttSNException e) {
            LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Failed sending Mqtt PUBACK message to the broker.",e);
            connectionLost();
        }
    }

    /**
     * 处理发布环节消息
     * @param receive
     */
    private void handleMqttSNPubComp(MqttSNPubComp receive) {
        if (!client.isConnected()) {
            sendClientDisconnect();
            return;
        }
        MqttPubComp pubComp = new MqttPubComp();
        pubComp.setMsgId(receive.getMsgId());

        try {
            broker.sendMsg(pubComp);
        } catch (MqttSNException e) {
            LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Failed sending Mqtt PUBCOMP message to the broker.",e);
            connectionLost();
        }
    }

    /**
     * 处理MQTT-SN pubrec消息
     * @param receive
     */
    private void handleMqttSNPubRec(MqttSNPubRec receive) {

        if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received MqttSN PUBREC message cannot be processed.");
            sendClientDisconnect();
            return;
        }

        //构建一个 Mqtt PUBREC 消息
        MqttPubRec pubrec = new MqttPubRec();
        pubrec.setMsgId(receive.getMsgId());

        try {
            broker.sendMsg(pubrec);
        } catch (MqttSNException e) {
            LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Failed sending Mqtt PUBREC message to the broker.");
            connectionLost();
        }
    }

    /**
     *
     * @param receive
     */
    private void handleMqttSNPubRel(MqttSNPubRel receive) {
       if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received MqttSN PUBREL message cannot be processed.");
            sendClientDisconnect();
            return;
        }

        MqttPubRel pubrel = new MqttPubRel();
        pubrel.setMsgId(receive.getMsgId());

        LOG.info( "ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending Mqtt PUBREL message to the broker.");
        try {
            broker.sendMsg(pubrel);
        } catch (MqttSNException e) {
           LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Failed sending Mqtt PUBREL message to the broker.",e);
            connectionLost();
        }
    }

    /**
     *
     * @param receive
     */
    private void handleMqttSNSubscribe(MqttSNSubscribe receive) {
        if(receive.getTopicIdType() == MqttSNMessage.TOPIC_NAME)
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN SUBSCRIBE message with \"TopicName\" = \""+receive.getTopicName()+"\" received.");
        else if(receive.getTopicIdType() == MqttSNMessage.PREDIFINED_TOPIC_ID)
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN SUBSCRIBE message with \"TopicId\" = \""+receive.getPredefinedTopicId()+"\" (predefined topid Id) received.");
        else if(receive.getTopicIdType() == MqttSNMessage.SHORT_TOPIC_NAME)
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN SUBSCRIBE message with \"TopicId\" = \""+receive.getShortTopicName()+"\" (short topic name) received.");
        else{
            LOG.warn( "ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN SUBSCRIBE message with unknown topicIdType (\"" + receive.getTopicIdType()+"\") received. The message cannot be processed.");
            return;
        }

        if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received MqttSN SUBSCRIBE message cannot be processed.");
            sendClientDisconnect();
            return;
        }

       if(gateway.isWaitingSuback()){
           LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is already in a subscription procedure. The received MqttSN SUBSCRIBE message cannot be processed.");
            return;
        }

        MqttSubscribe mqttSubscribe = new MqttSubscribe();
        switch(receive.getTopicIdType()){
            case MqttSNMessage.TOPIC_NAME:
                mqttSubscribe.setTopicName(receive.getTopicName());
                break;
            case MqttSNMessage.SHORT_TOPIC_NAME:
                mqttSubscribe.setTopicName(receive.getShortTopicName());
                break;
            case MqttSNMessage.PREDIFINED_TOPIC_ID:
                if(receive.getPredefinedTopicId() > GWParameters.getPredfTopicIdSize()){
                LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Predefined topicId (\"" + + receive.getPredefinedTopicId() + "\") of the received MqttSN SUBSCRIBE message is out of the range of predefined topic Ids [1,"+GWParameters.getPredfTopicIdSize()+"]. The message cannot be processed. MqttSN SUBACK with rejection reason will be sent to the client.");
                    MqttSNSuback suback = new MqttSNSuback();
                    suback.setTopicIdType(MqttSNMessage.PREDIFINED_TOPIC_ID);
                    suback.setPredefinedTopicId(receive.getPredefinedTopicId());
                    suback.setMsgId(receive.getMsgId());
                    suback.setReturnCode(MqttSNMessage.RETURN_CODE_INVALID_TOPIC_ID);

                   LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN SUBACK message with \"TopicId\" = \"" +receive.getPredefinedTopicId()+"\" and \"ReturnCode\" = \"Rejected: invalid TopicId\" to the client.");

                    clientInterface.sendMsg(this.clientAddress, suback);

                    return;
                }
                String topicName = topicIdMappingTable.getTopicName(receive.getPredefinedTopicId());
                if(topicName == null){
                  LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Predefined topicId (\"" + receive.getPredefinedTopicId() + "\") of the received MqttSN SUBSCRIBE message does not exist. The message cannot be processed. MqttSN SUBACK with rejection reason will be sent to the client.");

                    MqttSNSuback suback = new MqttSNSuback();
                    suback.setTopicIdType(MqttSNMessage.PREDIFINED_TOPIC_ID);
                    suback.setPredefinedTopicId(receive.getPredefinedTopicId());
                    suback.setMsgId(receive.getMsgId());
                    suback.setReturnCode(MqttSNMessage.RETURN_CODE_INVALID_TOPIC_ID);

                    LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN SUBACK message with \"TopicId\" = \"" +receive.getPredefinedTopicId()+"\" and \"ReturnCode\" = \"Rejected: invalid TopicId\" to the client.");
                    clientInterface.sendMsg(this.clientAddress, suback);
                    return;
                }
                mqttSubscribe.setTopicName(topicName);
                break;

            default:
                LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Unknown topicIdType (\"" + receive.getTopicIdType()+"\"). The received MqttSN SUBSCRIBE message cannot be processed.");
                return;
        }

      this.mqttSNSubscribe = receive;
        mqttSubscribe.setDup(receive.isDup());
        mqttSubscribe.setMsgId(receive.getMsgId());
        mqttSubscribe.setRequestedQoS(receive.getQos());

        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending Mqtt SUBSCRIBE message with \"TopicName\" = \""+mqttSubscribe.getTopicName()+ "\" to the broker.");
        try {
            broker.sendMsg(mqttSubscribe);
        } catch (MqttSNException e) {
           LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Failed sending Mqtt SUBSCRIBE message to the broker.",e);
            connectionLost();
            return;
        }
        gateway.setWaitingSuback();
    }

    /**
     *
     * @param receive
     */
    private void handleMqttSNUnsubscribe(MqttSNUnsubscribe receive) {
        if(receive.getTopicIdType() == MqttSNMessage.TOPIC_NAME)
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN UNSUBSCRIBE message with \"TopicName\" = \""+receive.getTopicName()+"\" received.");
        else if(receive.getTopicIdType() == MqttSNMessage.PREDIFINED_TOPIC_ID)
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN UNSUBSCRIBE message with \"TopicId\" = \""+receive.getPredefinedTopicId()+"\" (predefined topid Id) received.");
        else if(receive.getTopicIdType() == MqttSNMessage.SHORT_TOPIC_NAME)
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN UNSUBSCRIBE message with \"TopicId\" = \""+receive.getShortTopicName()+"\" (short topic name) received.");
        else{
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN UNSUBSCRIBE message with unknown topicIdType (\"" + receive.getTopicIdType()+"\") received. The message cannot be processed.");
            return;
        }

       if(!client.isConnected()){
           LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received MqttSN UNSUBSCRIBE message cannot be processed.");
            sendClientDisconnect();
            return;
        }
        if(gateway.isWaitingUnsuback()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is already in a un-subscription procedure. The received MqttSN UNSUBSCRIBE message cannot be processed.");
            return;
        }
        MqttUnsubscribe mqttUnsubscribe = new MqttUnsubscribe();
        switch(receive.getTopicIdType()){
            case MqttSNMessage.TOPIC_NAME:
                mqttUnsubscribe.setTopicName(receive.getTopicName());
                break;
            case MqttSNMessage.SHORT_TOPIC_NAME:
                mqttUnsubscribe.setTopicName(receive.getShortTopicName());
                break;
            case MqttSNMessage.PREDIFINED_TOPIC_ID:
                if(receive.getPredefinedTopicId() > GWParameters.getPredfTopicIdSize()){
                   LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Predefined topicId (\"" + + receive.getPredefinedTopicId() + "\") of the received MqttSN UNSUBSCRIBE message is out of the range of predefined topic Ids [1,"+GWParameters.getPredfTopicIdSize()+"]. The message cannot be processed.");
                    return;
                }
                String topicName = topicIdMappingTable.getTopicName(receive.getPredefinedTopicId());
                if(topicName == null){
                   LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Predefined topicId (\"" + + receive.getPredefinedTopicId() + "\") does not exist. The received MqttSN UNSUBSCRIBE message cannot be processed.");
                    return;
                }
                mqttUnsubscribe.setTopicName(topicName);
                break;
            default:
                LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Unknown topicIdType (\"" + receive.getTopicIdType()+"\"). The received MqttSN UNSUBSCRIBE message cannot be processed.");
                return;
        }
        this.mqttSNUnsubscribe = receive;
        mqttUnsubscribe.setDup(receive.isDup());
        mqttUnsubscribe.setMsgId(receive.getMsgId());
       LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending Mqtt UNSUBSCRIBE message with \"TopicName\" = \""+mqttUnsubscribe.getTopicName()+ "\" to the broker.");
        try {
            broker.sendMsg(mqttUnsubscribe);
        } catch (MqttSNException e) {
            LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Failed sending Mqtt UNSUBSCRIBE message to the broker.",e);
            connectionLost();
            return;
        }
        gateway.setWaitingUnsuback();
    }

    /**
     *
     * @param receive
     */
    private void handleMqttSNPingReq(MqttSNPingReq receive) {
      if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received MqttSN PINGREQ message cannot be processed.");
            sendClientDisconnect();
            return;
        }
        MqttPingReq pingreq = new MqttPingReq();
        try {
            broker.sendMsg(pingreq);
        } catch (MqttSNException e) {
            LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Failed sending Mqtt PINGREQ message to the broker.",e);
            connectionLost();
        }
    }

    /**
     *
     * @param receive
     */
    private void handleMqttSNPingResp(MqttSNPingResp receive) {
        if(!client.isConnected()){
           sendClientDisconnect();
            return;
        }
        MqttPingResp pingresp = new MqttPingResp();
        try {
            broker.sendMsg(pingresp);
        } catch (MqttSNException e) {
            LOG.error("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Failed sending Mqtt PINGRESP message to the broker.",e);
            connectionLost();
        }
    }

    /**
     *
     * @param received
     */
    private void handleMqttSNDisconnect(MqttSNDisconnect received) {
        if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received MqttSN DISCONNECT message cannot be processed.");
            return;
        }
        broker.setRunning(false);
        MqttDisconnect mqttDisconnect = new MqttDisconnect();
        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending Mqtt DISCONNECT message to the broker.");
        try {
            broker.sendMsg(mqttDisconnect);
        } catch (MqttSNException e) {
            //do nothing
        }
        sendClientDisconnect();
    }

    /**
     *
     * @param receivedMsg
     */
    private void handleMqttSNWillTopicUpd(MqttSNWillTopicUpd receivedMsg) {
        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN WILLTOPICUPD message received.");
    }

    /**
     *
     * @param receivedMsg
     */
    private void handleMqttSNWillMsgUpd(MqttSNWillMsgUpd receivedMsg) {
        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MqttSN WILLMSGUPD received.");
    }

    /**
     *
     * @param receivedMsg
     */
    private void handleMqttConnack(MqttConnack receivedMsg) {
        if(!client.isConnected()){
           LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received Mqtt CONNACK message cannot be processed.");
            return;
        }
        if (receivedMsg.getReturnCode() != MqttMessage.RETURN_CODE_CONNECTION_ACCEPTED){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Return Code of Mqtt CONNACK message it is not \"Connection Accepted\". The received Mqtt CONNACK message cannot be processed.");
            sendClientDisconnect();
            return;
        }
        MqttSNConnack msg = new MqttSNConnack();
        msg.setReturnCode(MqttSNMessage.RETURN_CODE_ACCEPTED);
       LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN CONNACK message to the client.");
        clientInterface.sendMsg(this.clientAddress, msg);
    }


    /**
     *
     * @param receivedMsg
     */
    private void handleMqttPublish(MqttPublish receivedMsg) {
        if(!client.isConnected()){
           LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received Mqtt PUBLISH message cannot be processed.");
            return;
        }

        //if the data is too long to fit into a MqttSN PUBLISH message or the topic name is too
        //long to fit into a MqttSN REGISTER message then drop the received message
        if (receivedMsg.getPayload().length > GWParameters.getMaxMqttSNLength() - 7){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - The payload in the received Mqtt PUBLISH message does not fit into a MqttSN PUBLISH message (payload length = "+receivedMsg.getPayload().length+ ". The message cannot be processed.");
            return;
        }

        if (receivedMsg.getTopicName().length() > GWParameters.getMaxMqttSNLength() - 6){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - The topic name in the received Mqtt PUBLISH message does not fit into a MqttSN REGISTER message (topic name length = "+receivedMsg.getTopicName().length()+ ". The message cannot be processed.");
            return;
        }
        int topicId = topicIdMappingTable.getTopicId(receivedMsg.getTopicName());

        MqttSNPublish publish = new MqttSNPublish();
        if(topicId != 0){
            publish.setDup(receivedMsg.isDup());
            publish.setQos(receivedMsg.getQos());
            publish.setRetain(receivedMsg.isRetain());
            publish.setMsgId(receivedMsg.getMsgId());
            publish.setData(receivedMsg.getPayload());
            if(topicId > GWParameters.getPredfTopicIdSize()){
                publish.setTopicIdType(MqttSNMessage.NORMAL_TOPIC_ID);
                publish.setTopicId(topicId);
                LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN PUBLISH message with \"QoS\" = \""+receivedMsg.getQos()+ "\" and \"TopicId\" = \"" + topicId + "\" to the client.");

            }
            else if (topicId>0 && topicId<=GWParameters.getPredfTopicIdSize()){
                publish.setTopicIdType(MqttSNMessage.PREDIFINED_TOPIC_ID);
                publish.setTopicId(topicId);
               LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN PUBLISH message with \"QoS\" = \""+receivedMsg.getQos()+ "\" and \"TopicId\" = \"" + topicId + "\" to the client.");

            }
            clientInterface.sendMsg(this.clientAddress, publish);
            return;
        }
        if (topicId == 0 && receivedMsg.getTopicName().length() == 2){
            publish.setTopicIdType(MqttSNMessage.SHORT_TOPIC_NAME);
            publish.setShortTopicName(receivedMsg.getTopicName());
            publish.setDup(receivedMsg.isDup());
            publish.setQos(receivedMsg.getQos());
            publish.setRetain(receivedMsg.isRetain());
            publish.setMsgId(receivedMsg.getMsgId());
            publish.setData(receivedMsg.getPayload());
           LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN PUBLISH message with \"QoS\" = \""+receivedMsg.getQos()+ "\" and \"TopicId\" = \"" + receivedMsg.getTopicName() + "\" (short topic name) to the client.");
            return;
        }

        //if topicId doesn't exist and we are not already in a register procedure initiated by
        //the gateway, then store the MqttSN PUBLISH and send a MqttSN REGISTER to the client
        if (topicId == 0 && !gateway.isWaitingRegack()){
            this.mqttPublish = receivedMsg;
            topicId = getNewTopicId();
            this.mqttSNRegister = new MqttSNRegister();
            this.mqttSNRegister.setTopicId(topicId);
            this.mqttSNRegister.setMsgId(getNewMsgId());
            this.mqttSNRegister.setTopicName(receivedMsg.getTopicName());
            clientInterface.sendMsg(this.clientAddress, mqttSNRegister);
            gateway.setWaitingRegack();
            gateway.increaseTriesSendingRegister();
            timer.register(this.clientAddress, ControlMessage.WAITING_REGACK_TIMEOUT, GWParameters.getWaitingTime());
            return;
        }
        if (topicId == 0 && gateway.isWaitingRegack()){
           LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Topic name (\"" +receivedMsg.getTopicName()+"\") does not exist in the mapping table and the gateway is waiting a MqttSN REGACK message from the client. The received Mqtt PUBLISH message cannot be processed.");
            return;
        }
    }

    /**
     *
     * @param receivedMsg
     */
    private void handleMqttPuback(MqttPuback receivedMsg) {
        if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received Mqtt PUBACK message cannot be processed.");
            return;
        }

        if(!gateway.isWaitingPuback()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Gateway is not waiting a Mqtt PUBACK message from the broker.The received message cannot be processed.");
            return;
        }
        if (this.mqttSNPublish == null){
           LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - The stored MqttSN PUBLISH message is null.The received Mqtt PUBACK message cannot be processed.");
            gateway.resetWaitingPuback();
            return;
        }

        //if the MsgId of the received Mqtt PUBACK is not the same with MsgId of the stored
        //MqttSN PUBLISH message, drop the received message and return (don't delete any stored message)
        if(receivedMsg.getMsgId() != this.mqttSNPublish.getMsgId()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Message ID of the received Mqtt PUBACK does not match the message ID of the stored MqttSN PUBLISH message.The message cannot be processed.");
            return;
        }

        MqttSNPuback puback = new MqttSNPuback();

        puback.setMsgId(receivedMsg.getMsgId());
        puback.setReturnCode(MqttSNMessage.RETURN_CODE_ACCEPTED);
        switch(this.mqttSNPublish.getTopicIdType()){
            case MqttSNMessage.NORMAL_TOPIC_ID:
                puback.setTopicId(this.mqttSNPublish.getTopicId());
                break;
            case MqttSNMessage.SHORT_TOPIC_NAME:
                puback.setShortTopicName(this.mqttSNPublish.getShortTopicName());
                break;
            case MqttSNMessage.PREDIFINED_TOPIC_ID:
                puback.setTopicId(this.mqttSNPublish.getTopicId());
                break;
            default:
               LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Unknown topicIdType of the stored MqttSN PUBLISH message: " + this.mqttSNPublish.getTopicIdType()+". The received Mqtt PUBACK message cannot be processed.");
                return;
        }
        clientInterface.sendMsg(this.clientAddress, puback);
        gateway.resetWaitingPuback();
        this.mqttSNPublish = null;
    }

    /**
     *
     * @param receivedMsg
     */
    private void handleMqttPubRec(MqttPubRec receivedMsg) {
        if(!client.isConnected()){
           LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received Mqtt PUBREC message cannot be processed.");
            return;
        }
        MqttSNPubRec msg = new MqttSNPubRec();
        msg.setMsgId(receivedMsg.getMsgId());

       LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN PUBREC message to the client.");
        clientInterface.sendMsg(this.clientAddress, msg);
    }

    /**
     *
     * @param receivedMsg
     */
    private void handleMqttPubRel(MqttPubRel receivedMsg) {
        if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received Mqtt PUBREL message cannot be processed.");
            return;
        }
        MqttSNPubRel msg = new MqttSNPubRel();
        msg.setMsgId(receivedMsg.getMsgId());
        clientInterface.sendMsg(this.clientAddress, msg);

    }

    /**
     *
     * @param receivedMsg
     */
    private void handleMqttPubComp(MqttPubComp receivedMsg) {

        if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received Mqtt PUBCOMP message cannot be processed.");
            return;
        }

        MqttSNPubComp msg = new MqttSNPubComp();
        msg.setMsgId(receivedMsg.getMsgId());
        clientInterface.sendMsg(this.clientAddress, msg);
    }

    /**
     *
     * @param receivedMsg
     */
    private void handleMqttSuback(MqttSuback receivedMsg) {

        if(!client.isConnected()){
           LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received Mqtt SUBACK message cannot be processed.");
            return;
        }
        if(!gateway.isWaitingSuback()){
           LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Gateway is not waiting a Mqtt SUBACK message from the broker. The received message cannot be processed.");
            return;
        }
        if (this.mqttSNSubscribe == null){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - The stored MqttSN SUBSCRIBE is null. The received Mqtt SUBACK message cannot be processed.");
            gateway.resetWaitingSuback();
            return;
        }

        //if the MsgId of the received Mqtt SUBACK is not the same with MsgId of the stored
        //MqttSN SUBSCRIBE message, drop the received message and return (don't delete any stored message)
        if(receivedMsg.getMsgId() != this.mqttSNSubscribe.getMsgId()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MsgId (\""+receivedMsg.getMsgId()+"\") of the received MqttSN SUBACK message does not match the MsgId (\""+this.mqttSNSubscribe.getMsgId()+"\") of the stored MqttSN SUBSCRIBE message. The message cannot be processed.");
            return;
        }
        MqttSNSuback suback = new MqttSNSuback();
        suback.setGrantedQoS(receivedMsg.getGrantedQoS());
        suback.setMsgId(receivedMsg.getMsgId());
        suback.setReturnCode(MqttSNMessage.RETURN_CODE_ACCEPTED);

        switch(this.mqttSNSubscribe.getTopicIdType()){
            case MqttSNMessage.TOPIC_NAME:
                suback.setTopicIdType(MqttSNMessage.NORMAL_TOPIC_ID);

                //if contains wildcard characters
                if(this.mqttSNSubscribe.getTopicName().equals("#")
                        || this.mqttSNSubscribe.getTopicName().equals("+")
                        || this.mqttSNSubscribe.getTopicName().indexOf("/#/") != -1
                        || this.mqttSNSubscribe.getTopicName().indexOf("/+/") != -1
                        || this.mqttSNSubscribe.getTopicName().endsWith("/#")
                        || this.mqttSNSubscribe.getTopicName().endsWith("/+")
                        || this.mqttSNSubscribe.getTopicName().startsWith("#/")
                        || this.mqttSNSubscribe.getTopicName().startsWith("+/")){
                    suback.setTopicId(0);
                }else if(topicIdMappingTable.getTopicId(this.mqttSNSubscribe.getTopicName()) != 0)
                    suback.setTopicId(topicIdMappingTable.getTopicId(this.mqttSNSubscribe.getTopicName()));
                else{
                    int topicId = getNewTopicId();
                    topicIdMappingTable.assignTopicId(topicId, this.mqttSNSubscribe.getTopicName());
                    suback.setTopicId(topicId);
                }
                LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN SUBACK message with \"TopicId\" = \"" + suback.getTopicId() + "\" to the client.");
                break;
            case MqttSNMessage.SHORT_TOPIC_NAME:
                suback.setTopicIdType(MqttSNMessage.SHORT_TOPIC_NAME);
                suback.setShortTopicName(this.mqttSNSubscribe.getShortTopicName());
                LOG.info( "ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN SUBACK message with \"TopicId\" = \"" + suback.getShortTopicName() + "\" (short topic name) to the client.");
                break;

            case MqttSNMessage.PREDIFINED_TOPIC_ID:
                suback.setTopicIdType(MqttSNMessage.PREDIFINED_TOPIC_ID);
                suback.setPredefinedTopicId(this.mqttSNSubscribe.getPredefinedTopicId());
                LOG.info( "ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN SUBACK message with \"TopicId\" = \"" + suback.getPredefinedTopicId() + "\" to the client.");
                break;
            default:
                LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - UnknownTopicId type of the stored MqttSN SUBSCRIBE message: " + this.mqttSNSubscribe.getTopicIdType()+". The received Mqtt SUBACK message cannot be processed.");
                return;
        }
        clientInterface.sendMsg(this.clientAddress, suback);
        gateway.resetWaitingSuback();
        this.mqttSNSubscribe = null;
    }

    /**
     *
     * @param receivedMsg
     */
    private void handleMqttUnsuback(MqttUnsuback receivedMsg) {
        if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received Mqtt UNSUBACK message cannot be processed.");
            return;
        }
        if(!gateway.isWaitingUnsuback()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Gateway is not waiting a Mqtt UNSUBACK message from the broker.The received message cannot be processed.");
            return;
        }

        if (this.mqttSNUnsubscribe == null){
            LOG.warn( "ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - The stored MqttSN UNSUBSCRIBE is null.The received Mqtt UNSUBACK message cannot be processed.");

            gateway.resetWaitingUnsuback();
            return;
        }

        if(receivedMsg.getMsgId() != this.mqttSNUnsubscribe.getMsgId()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - MsgId (\""+receivedMsg.getMsgId()+"\") of the received MqttSN UNSUBACK message does not match the MsgId (\""+this.mqttSNUnsubscribe.getMsgId()+"\") of the stored MqttSN UNSUBSCRIBE message. The message cannot be processed.");
            return;
        }

        if(!(this.mqttSNUnsubscribe.getTopicIdType() == MqttSNMessage.SHORT_TOPIC_NAME || this.mqttSNUnsubscribe.getTopicIdType() == MqttSNMessage.PREDIFINED_TOPIC_ID))
            topicIdMappingTable.removeTopicId(this.mqttSNUnsubscribe.getTopicName());

        MqttSNUnsuback unsuback = new MqttSNUnsuback();
        unsuback.setMsgId(receivedMsg.getMsgId());

        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN UNSUBACK message to the client.");
        clientInterface.sendMsg(this.clientAddress, unsuback);
        gateway.resetWaitingUnsuback();
        mqttSNUnsubscribe = null;
    }

    /**
     *
     * @param receivedMsg
     */
    private void handleMqttPingReq(MqttPingReq receivedMsg) {
        if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received Mqtt PINGREQ message cannot be processed.");
            return;
        }
        MqttSNPingReq msg = new MqttSNPingReq();

       LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN PINGREQ message to the client.");
        clientInterface.sendMsg(this.clientAddress, msg);
    }

    /**
     *
     * @param receivedMsg
     */
    private void handleMqttPingResp(MqttPingResp receivedMsg) {
        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Mqtt PINGRESP message received.");

        if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received Mqtt PINGRESP message cannot be processed.");
            return;
        }

        MqttSNPingResp msg = new MqttSNPingResp();

        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending MqttSN PINGRESP message to the client.");
        clientInterface.sendMsg(this.clientAddress, msg);
    }

    /**
     *
     */
    private void handleWaitingWillTopicTimeout(){
        if(!gateway.isWaitingWillTopic()){
          LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Gateway is not waiting a MqttSN WILLTOPIC message from the client. The received control WAITING_WILLTOPIC_TIMEOUT message cannot be processed.");
            return;
        }
        if(gateway.getTriesSendingWillTopicReq() > GWParameters.getMaxRetries()){
           LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Maximum retries of sending MqttSN WILLTOPICREQ message to the client were reached. The message will not be sent again.");
            gateway.resetWaitingWillTopic();
            gateway.resetTriesSendingWillTopicReq();
            timer.unregister(this.clientAddress, ControlMessage.WAITING_WILLTOPIC_TIMEOUT);
            this.mqttSNConnect = null;
        }else{
            MqttSNWillTopicReq willTopicReq = new MqttSNWillTopicReq();
            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Re-sending MqttSN WILLTOPICREQ message to the client. Retry: "+gateway.getTriesSendingWillTopicReq()+".");
            clientInterface.sendMsg(this.clientAddress, willTopicReq);
            gateway.increaseTriesSendingWillTopicReq();
        }
    }

    /**
     *
     */
    private void handleWaitingWillMsgTimeout(){
        if(!gateway.isWaitingWillMsg()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Gateway is not waiting a MqttSN WILLMSG message from the client. The received control WAITING_WILLMSG_TIMEOUT message cannot be processed.");
            return;
        }
        if(gateway.getTriesSendingWillMsgReq() > GWParameters.getMaxRetries()){
            gateway.resetWaitingWillMsg();
            gateway.resetTriesSendingWillMsgReq();
            timer.unregister(this.clientAddress, ControlMessage.WAITING_WILLMSG_TIMEOUT);
            this.mqttSNConnect = null;
            this.mqttSNWillTopic = null;

        }else{
            MqttSNWillMsgReq willMsgReq = new MqttSNWillMsgReq();

            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Re-sending MqttSN WILLMSGREQ message to the client. Retry: "+gateway.getTriesSendingWillMsgReq()+".");
            clientInterface.sendMsg(this.clientAddress, willMsgReq);
            gateway.increaseTriesSendingWillMsgReq();
        }
    }


    /**
     *
     */
    private void handleWaitingRegackTimeout(){
        if(!gateway.isWaitingRegack()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Gateway is not in state of waiting a MqttSN REGACK message from the client. The received control REGACK_TIMEOUT message cannot be processed.");
            return;
        }
        if(gateway.getTriesSendingRegister() > GWParameters.getMaxRetries()){
            gateway.resetWaitingRegack();
            gateway.resetTriesSendingRegister();
            timer.unregister(this.clientAddress, ControlMessage.WAITING_REGACK_TIMEOUT);
            this.mqttPublish = null;
            this.mqttSNRegister = null;
        }
        else{
            this.mqttSNRegister.setMsgId(getNewMsgId());

            LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Re-sending MqttSN REGISTER message to the client. Retry: "+gateway.getTriesSendingRegister()+".");
            clientInterface.sendMsg(this.clientAddress, this.mqttSNRegister);
            gateway.increaseTriesSendingRegister();
        }
    }

    /**
     *
     */
    private void handleCheckInactivity() {
        LOG.info(
                "ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+
                        "]/["+clientId+"] - Control CHECK_INACTIVITY message received.");

        if(System.currentTimeMillis() > this.timeout){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is inactive for more than "+ GWParameters.getHandlerTimeout()/60+ " minutes. The associated ClientMsgHandler will be removed from Dispatcher's mapping table.");
            broker.disConnect();
            dispatcher.removeHandler(this.clientAddress);
        }
    }

    /**
     *
     */
    private void shutDown(){
        LOG.info("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Control SHUT_DOWN message received.");
        if(!client.isConnected()){
            LOG.warn("ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Client is not connected. The received Control SHUT_DOWN message cannot be processed.");
            return;
        }
        broker.setRunning(false);
        MqttDisconnect mqttDisconnect = new MqttDisconnect();

        LOG.info( "ClientMsgHandler ["+Utils.hexString(this.clientAddress.getAddress())+"]/["+clientId+"] - Sending Mqtt DISCONNECT message to the broker.");
        try {
            broker.sendMsg(mqttDisconnect);
        } catch (MqttSNException e) {
            // do nothing
        }
    }

    /**
     * 连接丢失
     */
    private void connectionLost() {
        if (!client.isConnected()) {
            return;
        }
        sendClientDisconnect();
    }

    public void setClientInterface(ClientInterface clientInterface) {
        this.clientInterface = clientInterface;
    }

    private int getNewTopicId() {
        return topicId ++;
    }

    private int getNewMsgId() {
        return msgId ++;
    }

    /**
     * 处理断线操作
     */
    private void sendClientDisconnect() {
        MqttSNDisconnect mqttSNDisconnect = new MqttSNDisconnect();
        clientInterface.sendMsg(this.clientAddress,mqttSNDisconnect);
        client.setDisconnected();
        timer.unregister(this.clientAddress);
        gateway.reset();
        broker.disConnect();
    }

    /**
     * 客户端测试
     */
    private class ClientState {

        private final int NOT_CONNECTED = 1;
        private final int CONNECTED 	= 2;
        private final int DISCONNECTED 	= 3;

        private int state;

        public ClientState(){
            state = NOT_CONNECTED;
        }

        public boolean isConnected() {
            return (state == CONNECTED);
        }

        public void setConnected() {
            state = CONNECTED;
        }

        public void setDisconnected() {
            state = DISCONNECTED;
        }
    }

    /**
     * The class that represents the state of the gateway at any given time.
     *
     */
    private class GatewayState {

        //waiting message from the client
        private boolean waitingWillTopic;
        private boolean waitingWillMsg;
        private boolean waitingRegack;

        //waiting message from the broker
        private boolean waitingSuback;
        private boolean waitingUnsuback;
        private boolean waitingPuback;

        //counters
        private int triesSendingWillTopicReq;
        private int triesSendingWillMsgReq;
        private int triesSendingRegister;

        public GatewayState(){
            this.waitingWillTopic = false;
            this.waitingWillMsg = false;
            this.waitingRegack = false;

            this.waitingSuback = false;
            this.waitingUnsuback = false;
            this.waitingPuback = false;

            this.triesSendingWillTopicReq = 0;
            this.triesSendingWillMsgReq = 0;
            this.triesSendingRegister = 0;
        }

        public void reset(){
            this.waitingWillTopic = false;
            this.waitingWillMsg = false;
            this.waitingRegack = false;

            this.waitingSuback = false;
            this.waitingUnsuback = false;
            this.waitingPuback = false;


            this.triesSendingWillTopicReq = 0;
            this.triesSendingWillMsgReq = 0;
            this.triesSendingRegister = 0;

            //delete also all stored messages (if any)
            mqttSNConnect = null;
            mqttSNWillTopic = null;
            mqttSNSubscribe = null;
            mqttSNUnsubscribe = null;
            mqttSNRegister = null;
            mqttSNPublish = null;
            mqttPublish = null;
        }


        public boolean isEstablishingConnection() {
            return (isWaitingWillTopic() || isWaitingWillMsg());
        }


        public boolean isWaitingWillTopic() {
            return this.waitingWillTopic;
        }

        public void setWaitingWillTopic() {
            this.waitingWillTopic = true;
        }

        public void resetWaitingWillTopic() {
            this.waitingWillTopic = false;
        }

        public boolean isWaitingWillMsg() {
            return this.waitingWillMsg;
        }

        public void setWaitingWillMsg() {
            this.waitingWillMsg = true;
        }

        public void resetWaitingWillMsg() {
            this.waitingWillMsg = false;
        }

        public boolean isWaitingRegack() {
            return this.waitingRegack;
        }

        public void setWaitingRegack() {
            this.waitingRegack = true;
        }

        public void resetWaitingRegack() {
            this.waitingRegack = false;
        }

        public boolean isWaitingSuback() {
            return this.waitingSuback;
        }

        public void setWaitingSuback() {
            this.waitingSuback = true;
        }

        public void resetWaitingSuback() {
            this.waitingSuback = false;
        }

        public boolean isWaitingUnsuback() {
            return this.waitingUnsuback;
        }

        public void setWaitingUnsuback() {
            this.waitingUnsuback = true;
        }

        public void resetWaitingUnsuback() {
            this.waitingUnsuback = false;
        }

        public boolean isWaitingPuback() {
            return this.waitingPuback;
        }

        public void setWaitingPuback() {
            this.waitingPuback = true;
        }

        public void resetWaitingPuback() {
            this.waitingPuback = false;
        }

        public int getTriesSendingWillTopicReq() {
            return this.triesSendingWillTopicReq;
        }

        public void increaseTriesSendingWillTopicReq() {
            this.triesSendingWillTopicReq ++;
        }

        public void resetTriesSendingWillTopicReq() {
            this.triesSendingWillTopicReq = 0;
        }

        public int getTriesSendingWillMsgReq() {
            return this.triesSendingWillMsgReq;
        }

        public void increaseTriesSendingWillMsgReq() {
            this.triesSendingWillMsgReq ++;
        }

        public void resetTriesSendingWillMsgReq() {
            this.triesSendingWillMsgReq = 0;
        }

        public int getTriesSendingRegister() {
            return this.triesSendingRegister;
        }

        public void increaseTriesSendingRegister() {
            this.triesSendingRegister ++;
        }

        public void resetTriesSendingRegister() {
            this.triesSendingRegister = 0;
        }
    }
}
