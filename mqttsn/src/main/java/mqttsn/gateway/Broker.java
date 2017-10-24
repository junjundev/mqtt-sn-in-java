package mqttsn.gateway;

import mqttsn.gateway.messages.ControlMessage;
import mqttsn.gateway.messages.Message;
import mqttsn.gateway.messages.MqttMessage;
import mqttsn.gateway.messages.mqttType.*;
import mqttsn.gateway.utils.Address;
import mqttsn.gateway.utils.Dispatcher;
import mqttsn.gateway.utils.GWParameters;
import mqttsn.gateway.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;


public class Broker implements BrokerInterface,Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Broker.class);

    private DataInputStream inputStream = null;
    private DataOutputStream outputStream = null;
    private Socket socket;
    private Address address;
    private String brokerURL;
    private int port;
    private String clientId;

    private volatile boolean running;
    private Thread readThread;
    private Dispatcher dispatcher;
    public static final int MAX_HDR_LENGTH = 5;
    public static final int MAX_MSG_LENGTH = 268435455;

    public Broker(Address address) {
        this.address = address;
        this.brokerURL = GWParameters.getBrokerURL();
        this.port = GWParameters.getBrokerTcpPort();
        this.running = false;
        this.readThread = null;
        this.dispatcher = Dispatcher.getInstance();
    }

    public void run() {
        while (running) {
            readMsg();
        }
    }

    public void initialize() throws MqttSNException {
        try {
            socket = new Socket(brokerURL, port);
            inputStream = new DataInputStream(socket.getInputStream());
            outputStream = new DataOutputStream(socket.getOutputStream());

        } catch (UnknownHostException e) {
            disConnect();
            throw new MqttSNException(e.getMessage());
        } catch (IOException e) {
            disConnect();
            throw new MqttSNException(e.getMessage());
        }

        //create thread for reading
        this.readThread = new Thread (this, "Broker");
        this.running = true;
        this.readThread.start();
    }

    public void readMsg() {
        byte [] body = null;

        // read the header from the input stream
        MqttHeader hdr = new MqttHeader();
        hdr.header = new byte[MAX_HDR_LENGTH];

        if (this.inputStream == null){
            return;
        }

        try{
            int res = inputStream.read();
            hdr.header[0]=(byte) res;
            hdr.headerLength=1;
            if(res==-1) {
                throw new EOFException();
            }
            // read the Mqtt length
            int multiplier = 1;
            hdr.remainingLength=0;
            do {
                //read MsgLength bytes
                res = inputStream.read();
                if(res==-1) {
                    throw new EOFException();
                }
                hdr.header[hdr.headerLength++] = (byte) res;
                hdr.remainingLength += (res & 127) * multiplier;
                multiplier *= 128;
            } while ((res & 128) != 0 && hdr.headerLength<MAX_HDR_LENGTH);

            //some checks
            if (hdr.headerLength > MAX_HDR_LENGTH || hdr.remainingLength > MAX_MSG_LENGTH || hdr.remainingLength < 0) {
                LOG.warn("Broker ["+ Utils.hexString(this.address.getAddress())+"]/["+clientId+"] - Not a valid MqttSN message.");
                return;
            }

            body = new byte[hdr.remainingLength+hdr.headerLength];

            for (int i = 0; i < hdr.headerLength; i++) {
                body[i] = hdr.header[i];
            }

            if (hdr.remainingLength >= 0) {
                inputStream.readFully(body, hdr.headerLength, hdr.remainingLength);
            }

            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(body!=null)
                decodeMsg(body);
        }catch(IOException e){
            if(e instanceof InterruptedIOException) {
                //do nothing
            }else if(this.running == true){
                //an error occurred
                //stop the reading thread
                this.running = false;

                //generate a control message
                ControlMessage controlMsg = new ControlMessage();
                controlMsg.setMsgType(ControlMessage.CONNECTION_LOST);

                Message msg = new Message(this.address);
                msg.setType(Message.CONTROL_MSG);
                msg.setControlMessage(controlMsg);
                this.dispatcher.putMessage(msg);
            }
        }
    }

    public void sendMsg(MqttMessage message) throws MqttSNException {
        if (this.outputStream != null) {
            try {
                this.outputStream.write(message.toBytes());
                this.outputStream.flush();
            } catch (IOException e) {
                disConnect();
                throw new MqttSNException(e.getMessage());
            }
        }else{
            disConnect();
            throw new MqttSNException("Writing stream is null!");
        }
    }

    public void disConnect() {
        //stop the reading thread (if any)
        this.running = false;

        //close the out stream
        if (this.outputStream != null) {
            try {
                this.outputStream.flush();
                this.outputStream.close();
            } catch (IOException e) {
                // ignore it
            }
            this.outputStream = null;
        }

        //close the in stream
        if (this.inputStream != null) {
            try {
                this.inputStream.close();
            } catch (IOException e) {
                // ignore it
            }
            inputStream = null;
        }

        //close the socket
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
            }
            socket = null;
        }
    }

    private void decodeMsg(byte[] data){
        MqttMessage mqttMsg = null;
        int msgType = (data[0] >>> 4) & 0x0F;
        switch (msgType) {
            case MqttMessage.CONNECT:
                // we will never receive such a message from the broker
                break;

            case MqttMessage.CONNACK:
                mqttMsg = new MqttConnack(data);
                break;

            case MqttMessage.PUBLISH:
                mqttMsg = new MqttPublish(data);
                break;

            case MqttMessage.PUBACK:
                mqttMsg = new MqttPuback(data);
                break;

            case MqttMessage.PUBREC:
                mqttMsg = new MqttPubRec(data);
                break;

            case MqttMessage.PUBREL:
                mqttMsg = new MqttPubRel(data);
                break;

            case MqttMessage.PUBCOMP:
                mqttMsg = new MqttPubComp(data);
                break;

            case MqttMessage.SUBSCRIBE:
                //we will never receive such a message from the broker
                break;

            case MqttMessage.SUBACK:
                mqttMsg = new MqttSuback(data);
                break;

            case MqttMessage.UNSUBSCRIBE:
                //we will never receive such a message from the broker
                break;

            case MqttMessage.UNSUBACK:
                mqttMsg = new MqttUnsuback(data);
                break;

            case MqttMessage.PINGREQ:
                mqttMsg = new MqttPingReq(data);
                break;

            case MqttMessage.PINGRESP:
                mqttMsg = new MqttPingResp(data);
                break;

            case MqttMessage.DISCONNECT:
                //we will never receive such a message from the broker
                break;

            default:
                LOG.warn("Broker ["+Utils.hexString(this.address.getAddress())+"]/["+clientId+"] - Mqtt message of unknown type \"" + msgType+"\" received.");
                break;
        }

        Message msg = new Message(this.address);
        msg.setType(Message.MQTT_MSG);
        msg.setMqttMessage(mqttMsg);
        this.dispatcher.putMessage(msg);
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    /**
     * @param clientId
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }


    /**
     * This class represents a Mqtt header and is used for decoding a Mqtt message
     * from the broker.
     */
    public static class MqttHeader {
        public byte[]	header;
        public int remainingLength;
        public int headerLength;
    }
}
