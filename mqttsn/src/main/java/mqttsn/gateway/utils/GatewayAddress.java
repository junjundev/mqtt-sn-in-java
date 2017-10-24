package mqttsn.gateway.utils;

import java.net.InetAddress;

public class GatewayAddress extends Address {
    private byte[] gatewayAddress = null;
    private InetAddress ipAddress = null;
    private int port = 0;

    public GatewayAddress(byte[] addr) {
        this.gatewayAddress = addr;
        this.ipAddress = null;
        this.port = 0;
    }

    public GatewayAddress(byte[] addr, InetAddress ipAddr, int port) {
        this.gatewayAddress = addr;
        this.ipAddress = ipAddr;
        this.port   = port;
    }
    @Override
    public boolean equal(Object o) {
        if(o == null) return false;
        if(!(o instanceof GatewayAddress)) return false;
        if(o == this) return true;

        GatewayAddress ga = (GatewayAddress)o;
        if(gatewayAddress == null) {
            if(ga.gatewayAddress == null) {
                return true;
            } else {
                return false;
            }
        } else {
            if(ga.gatewayAddress == null || gatewayAddress.length != ga.gatewayAddress.length)
                return false;
            boolean ok = true;
            for(int i = 0; i < gatewayAddress.length; i++) {
                if(gatewayAddress[i] != ga.gatewayAddress[i]) {
                    ok = false;
                    break;
                }
            }
            if(!(ipAddress.equals(ga.getIPaddress()) && port == ga.getPort()))
                ok = false;
            return ok;
        }
    }

    @Override
    public void setIPaddress(Address addr) {
        GatewayAddress gatewayAddr = (GatewayAddress) addr;
        this.ipAddress = gatewayAddr.ipAddress;
        this.port   = gatewayAddr.port;
    }

    @Override
    public byte[] getAddress() {
        return this.gatewayAddress;
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
