package mqttsn.gateway.utils;

import java.util.LinkedList;

public class MsgQueue {

    private LinkedList<Object> queue = new LinkedList<Object>();

    public void addLast(Object o) {
        synchronized (queue) {
            queue.add(o);
            queue.notify();
        }
    }

    public void addFirst(Object o) {
        synchronized (queue) {
            queue.addFirst(o);
            queue.notify();
        }
    }
    public Object get() throws InterruptedException {
        synchronized (queue) {
            while (queue.isEmpty()) {
                queue.wait();
            }
            return queue.removeFirst();
        }
    }

    public int size() {
        return queue.size();
    }
}
