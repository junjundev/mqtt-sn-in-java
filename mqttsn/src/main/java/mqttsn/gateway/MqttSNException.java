package mqttsn.gateway;

public class MqttSNException extends Exception {
    private static final long serialVersionUID = 1L;

    private Throwable cause = null;

    public MqttSNException() {
        super();
    }

    public MqttSNException(String s) {
        super(s);
    }

    public MqttSNException(Throwable cause) {
        super((cause == null) ? null : cause.toString());
        this.cause = cause;
    }

    public Throwable getCause() {
        return this.cause;
    }
}
