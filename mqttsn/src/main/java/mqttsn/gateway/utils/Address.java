package mqttsn.gateway.utils;

import java.net.InetAddress;

public abstract class Address {
    public abstract boolean equal(Object o);

    public abstract void setIPaddress(Address addr);

    public abstract byte[] getAddress() ;

    public abstract InetAddress getIPaddress() ;

    public abstract int getPort();
}
