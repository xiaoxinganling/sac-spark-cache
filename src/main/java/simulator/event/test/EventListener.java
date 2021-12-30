package simulator.event.test;

public interface EventListener extends java.util.EventListener {

    //事件处理
    void handleEvent(EventObject event);
}
