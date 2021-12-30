package simulator.event.test;

import simulator.event.test.EventListener;
import simulator.event.test.EventObject;
import simulator.event.test.EventSource;

public class Test {
    public static void main(String[] args) {
        EventSource eventSource = new EventSource();
        eventSource.addListener(new EventListener() {
            @Override
            public void handleEvent(EventObject event) {
                event.doEvent();
                if (event.getSource().equals("closeWindows")) {
                    System.out.println("doClose");
                }
            }
        });

         /*
         * 传入openWindows事件，通知listener，事件监听器，
         对open事件感兴趣的listener将会执行
         **/
        eventSource.notifyListenerEvents(new EventObject("openWindows"));
    }
}
