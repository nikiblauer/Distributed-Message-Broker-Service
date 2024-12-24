package dslab.broker;

import java.util.HashSet;
import java.util.LinkedList;

public class Queue {
    private final LinkedList<String> queue;
    private final String name;
    public final TopicTrie trie = new TopicTrie();
    public final HashSet<String> bindings = new HashSet<>();

    public Queue(String name) {
        this.name = name;
        this.queue = new LinkedList<>();
    }

    public void bind(String bindingKey) {
        synchronized (this){
            trie.insertBindingKey(bindingKey);
            bindings.add(bindingKey);
        }

    }

    public boolean matchBinding(String routingKey){
        return trie.matchesRoutingKey(routingKey);
    }

    public boolean matchExactBinding(String routingKey){
        return bindings.contains(routingKey);
    }

    public String getName() {
        return name;
    }

    public void addMessage(String message) {
        synchronized (queue) {
            queue.add(message);
            queue.notifyAll(); // Notify any waiting threads that a new message is available
        }
    }


    public String getMessage() throws InterruptedException {
        synchronized (queue) {
            while (queue.isEmpty()) {
                queue.wait(); // Wait until a message is available
            }
            return queue.poll(); // Retrieve and remove the head of the queue
        }
    }

}
