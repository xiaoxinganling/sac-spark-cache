package simulator.event;

import simulator.event.test.EventListener;
import simulator.event.test.EventObject;

public class BaseEventListener implements EventListener {

    @Override
    public void handleEvent(EventObject event) {
        if(event.getSource() == EventType.StageSubmittedEvent) {
            System.out.println("stage submitted!");
        }
    }
}
