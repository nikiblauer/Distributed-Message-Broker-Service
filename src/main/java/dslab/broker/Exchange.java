package dslab.broker;
import java.util.*;

public class Exchange {
    private final ExchangeType type;
    private final String name;
    private final HashSet<Queue> queues;


    public Exchange(ExchangeType type, String name) {
        this.type = type;
        this.name = name;
        queues = new HashSet<>();
    }

    public ExchangeType getType() {
        return type;
    }

    public String getName() {
        return name;
    }
    public static ExchangeType convertType(String typeStr){
        return ExchangeType.valueOf(typeStr.toUpperCase());
    }

    public void bindQueue(String bindingKey, Queue queue) {
        synchronized (queues) {
            queue.bind(bindingKey);
            queues.add(queue);
        }
    }

    public List<Queue> routeMessage(String routingKey){
        List<Queue> targetQueues = new LinkedList<>();

        switch (type) {
            case DIRECT, DEFAULT:
                for (Queue queue : queues) {
                    if(queue.matchExactBinding(routingKey)){
                        targetQueues.add(queue);
                    }
                }
                break;
            case FANOUT:
                targetQueues.addAll(queues);
                break;
            case TOPIC:
                for (Queue queue : queues) {
                    if(queue.matchBinding(routingKey)){
                        targetQueues.add(queue);
                    }
                }
                break;
        }

        return targetQueues;
    }

}
