package simulator.event.test;

import java.util.Vector;

public class EventSource {
    //监听器列表，监听器的注册则加入此列表
    private Vector<EventListener> listeners = new Vector<EventListener>();

    //注册监听器
    public void addListener(EventListener eventListener){
        listeners.add(eventListener);
    }
    //撤销注册
    public void removeListener(EventListener eventListener){
        listeners.remove(eventListener);
    }

    /**
     * 接受外部事件
     * 当事件源对象上发生操作时，它将会调用事件监听器的一个方法，并在调用该方法时传递事件对象过去
     * @param eventObject
     */
    public void notifyListenerEvents(EventObject eventObject){
        for (EventListener listener : listeners) {
            listener.handleEvent(eventObject);
        }
    }
}