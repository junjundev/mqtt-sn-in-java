package mqttsn.gateway.utils;

import mqttsn.gateway.messages.ControlMessage;
import mqttsn.gateway.messages.Message;

import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

public class TimerService {

    private static TimerService instance = null;

    private Timer timer;
    private Dispatcher dispatcher;
    private Vector<TimeoutTimerTask> timeoutTasks;

    public TimerService() {
        timer = new Timer();
        dispatcher = Dispatcher.getInstance();
        timeoutTasks = new Vector<TimeoutTimerTask>();
    }

    public static synchronized TimerService getInstance() {
        if (instance == null) {
            instance = new TimerService();
        }
        return instance;
    }

    public void register(Address address, int type, int timeout) {
        long delay = timeout * 1000;
        long period = timeout * 1000;

        TimeoutTimerTask timeoutTimerTask = new TimeoutTimerTask(address,type);

        timeoutTasks.add(timeoutTimerTask);
        timer.scheduleAtFixedRate(timeoutTimerTask, delay, period);
    }

    public void unregister(Address address, int type) {
        for(int i = 0 ; i<timeoutTasks.size(); i++) {
            TimeoutTimerTask timeout = (TimeoutTimerTask)timeoutTasks.get(i);
            if(timeout.getAddress().equal(address))
                if (timeout.getType() == type){
                    timeoutTasks.remove(i);
                    timeout.cancel();
                    break;
                }
        }
    }

    public void unregister(Address address){
        for(int i = timeoutTasks.size()-1; i >= 0; i--) {
            TimeoutTimerTask timeout = (TimeoutTimerTask)timeoutTasks.get(i);
            if(timeout.getAddress().equal(address)){
                timeoutTasks.remove(i);
                timeout.cancel();
            }
        }
    }

    public class TimeoutTimerTask extends TimerTask {
        Address address;
        int type;

        public TimeoutTimerTask(Address addr, int type) {
            this.address = addr;
            this.type = type;
        }

        public void run(){

            ControlMessage controlMsg = new ControlMessage();
            controlMsg.setMsgType(type);

            Message msg = new Message(this.address);
            msg.setType(Message.CONTROL_MSG);
            msg.setControlMessage(controlMsg);
            dispatcher.putMessage(msg);
        }

        public Address getAddress() {
            return address;
        }

        public int getType() {
            return type;
        }
    }
}
