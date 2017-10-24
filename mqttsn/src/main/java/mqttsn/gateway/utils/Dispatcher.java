package mqttsn.gateway.utils;

import mqttsn.gateway.ClientMessageHandler;
import mqttsn.gateway.MsgHandler;
import mqttsn.gateway.messages.ControlMessage;
import mqttsn.gateway.messages.Message;
import mqttsn.gateway.messages.MqttMessage;
import mqttsn.gateway.messages.MqttSNMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
public class Dispatcher implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);

    private static Dispatcher instance = null;

    private MsgQueue dataQueue;
    private Hashtable<Address, MsgHandler> handlerTable;
    private volatile boolean running;
    private Thread readingThread;

    /**
     * 初始化方法
     */
    public void init() {
        dataQueue = new MsgQueue();
        handlerTable = new Hashtable<Address,MsgHandler>();
        this.running = true;
        this.readingThread = new Thread(this,"dispatcer");
        this.readingThread.start();
    }

    /**
     * 提供对外实例化方法
     * @return
     */
    public static synchronized Dispatcher getInstance(){
        if (instance ==null) {
            instance = new Dispatcher();
        }
        return instance;
    }

    /**
     * 根据队列中的消息类型从队列中读取消息
     */
    public void dispatch() {
        Message msg = null;
        try {
            msg = (Message) dataQueue.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int type = msg.getType();
        switch (type) {
            case Message.MQTTSN_MSG:
                dispatcherMqttSNMessage(msg);
                break;
            case Message.MQTT_MSG:
                dispatcherMqttMessage(msg);
                break;
            case Message.CONTROL_MSG:
                 dispatcherControlMessage(msg);
                 break;
            default:
                LOG.warn("Dispatcher - Message of unknown type \"" + msg.getType()+"\" received.");
                break;
        }
    }

    /**
     *
     * @param msg
     */
    private void dispatcherMqttSNMessage(Message msg) {
        Address address = msg.getAddress();
        MqttSNMessage mqttSNMessage = msg.getMqttSNMessage();
        if (mqttSNMessage == null) {
            LOG.warn("Dispatcher - The received MqttSN message is null. The message cannot be processed.");
            return;
        }
        MsgHandler handler = getHandler(address);

        if (handler == null) {
            ClientAddress clientAddress = (ClientAddress) address;
            handler = new ClientMessageHandler(clientAddress);
            putHandler(clientAddress,handler);
            handler.initialize();
        }
        if(handler instanceof ClientMessageHandler && msg.getClientInterface() != null){
            ClientMessageHandler clientHandler = (ClientMessageHandler) handler;
            clientHandler.setClientInterface(msg.getClientInterface());
        }
        handler.handleMqttSNMessage(mqttSNMessage);
    }

    /**
     *
     * @param msg
     */
    private void dispatcherMqttMessage(Message msg) {

        Address address = msg.getAddress();
        MqttMessage mqttMsg = msg.getMqttMessage();

        if(mqttMsg == null){
            LOG.warn("Dispatcher - The received Mqtt message is null. The message cannot be processed.");
            return;
        }

        MsgHandler handler = getHandler(address);

        if (handler == null){
            ClientAddress clientAddress = (ClientAddress) address;
            handler = new ClientMessageHandler(clientAddress);
            putHandler(clientAddress,handler);
            handler.initialize();
        }

        handler.handleMqttMessage(mqttMsg);
    }

    private void dispatcherControlMessage(Message msg) {
        Address address = msg.getAddress();
        ControlMessage controlMsg = msg.getControlMessage();

        if(controlMsg == null){
            LOG.warn("Dispatcher - The received Control message is null. The message cannot be processed.");
            return;
        }

        if(address == null){
           deliverMessageToAll(controlMsg);
            return;
        }
        MsgHandler handler = getHandler(address);

        if (handler == null){
            ClientAddress clientAddress = (ClientAddress) address;
            handler = new ClientMessageHandler(clientAddress);
            putHandler(clientAddress,handler);
            handler.initialize();
        }
        handler.handleControlMessage(controlMsg);
    }

    private void deliverMessageToAll(ControlMessage msg) {
        Enumeration<MsgHandler> values;
        MsgHandler handler;

        if(msg.getMsgType() == ControlMessage.SHUT_DOWN){
           LOG.info("-------- MqttSN Gateway shutting down --------");
        }
        for(values = handlerTable.elements(); values.hasMoreElements();){
            handler = (MsgHandler)values.nextElement();
            handler.handleControlMessage(msg);
        }

        if(msg.getMsgType() == ControlMessage.SHUT_DOWN){
            LOG.info("-------- MqttSN Gateway stopped --------");
            System.exit(0);
        }
    }

    /**
     *根据其地址从映射表获取一个msghandler
     * @param addr
     * @return
     */
    private MsgHandler getHandler(Address addr) {
        MsgHandler ret = null;
        Iterator<Address> iter = handlerTable.keySet().iterator();
        while (iter.hasNext()) {
            Address currentAddress = (Address)(iter.next());
            if(currentAddress.equal(addr) && addr.equal(currentAddress)) {
                currentAddress.setIPaddress(addr);
                ret = (MsgHandler)handlerTable.get(currentAddress);
                break;
            }
        }
        return ret;
    }

    public void putHandler(Address addr, MsgHandler handler) {
        this.handlerTable.put(addr, handler);
    }

    public void removeHandler(Address address) {
        Iterator<Address> iter  =  handlerTable.keySet().iterator();
        while (iter.hasNext()) {
            Address currentAddress = (Address)(iter.next());
            if(currentAddress.equal(address)){
                iter.remove();
                break;
            }
        }
    }

    public void putMessage(Message msg) {
        if(msg.getType() == Message.CONTROL_MSG)
            dataQueue.addFirst(msg);
        else
            dataQueue.addLast(msg);
    }

    public void run() {
        while(running){
            dispatch();
        }
    }
}
