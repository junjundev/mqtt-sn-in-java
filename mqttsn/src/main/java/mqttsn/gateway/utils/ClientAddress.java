package mqttsn.gateway.utils;

import java.net.InetAddress;

/**
 * 客户端连接的IP和PORT
 */
public class ClientAddress extends Address {

    private byte[] clientAddress = null;
    private InetAddress ipAddress = null;
    private int port = 0;
    private boolean isEncaps;
    private byte[] encaps;

    public ClientAddress(byte[] addr) {
        this.clientAddress = addr;
        this.ipAddress = null;
        this.port = 0;
        this.isEncaps = true;
    }

    public ClientAddress(byte[] addr, InetAddress ipAddr, int port, boolean isencaps, byte[] encaps) {
        this.clientAddress = addr;
        this.ipAddress = ipAddr;
        this.port   = port;
        this.isEncaps = isencaps;
        this.encaps = encaps;
    }

    public boolean isEncaps() {
        return isEncaps;
    }

    public byte[] getEncaps() {
        return encaps;
    }

    @Override
    public boolean equal(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof ClientAddress)) {
            return false;
        }
        if (o == this) {
            return true;
        }

        ClientAddress ca = (ClientAddress) o;
        if (clientAddress == null) {
            if (ca.clientAddress == null) {
                return true;
            } else {
                return false;
            }
        } else {
            if (ca.clientAddress == null || ca.clientAddress.length != clientAddress.length) {
                return false;
            }
            boolean flag = true;
            for (int i = 0; i < clientAddress.length;i++) {
                if (clientAddress[i] != ca.clientAddress[i]) {
                    flag = false;
                    break;
                }
            }
            return flag;
        }
    }

    @Override
    public void setIPaddress(Address addr) {
        ClientAddress clientAddr = (ClientAddress) addr;
        this.ipAddress = clientAddr.ipAddress;
        this.port = clientAddr.port;
    }

    @Override
    public byte[] getAddress() {
        return this.clientAddress;
    }

    @Override
    public InetAddress getIPaddress() {
        return this.ipAddress;
    }

    @Override
    public int getPort() {
        return this.port;
    }
}
