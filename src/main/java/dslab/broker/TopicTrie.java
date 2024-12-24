package dslab.broker;

public class TopicTrie {
    private final TrieNode root;

    public TopicTrie() {
        root = new TrieNode();
    }

    public String getPattern() {
        return "";
    }


    // Inserts a binding key into the Trie
    public void insertBindingKey(String bindingKey) {
        String[] parts = bindingKey.split("\\.");
        TrieNode current = root;
        for (String part : parts) {
            current = current.getOrCreateChild(part);
        }
        current.setIsEndOfPattern(true);  // Mark the end of the binding key

    }

    // Matches a routing key against stored binding patterns
    public boolean matchesRoutingKey(String routingKey) {
        String[] parts = routingKey.split("\\.");
        return match(parts, 0, root);
    }



    // Recursive helper to perform matching with wildcards
    private boolean match(String[] parts, int index, TrieNode node) {
        if (index == parts.length) {
            return node.isEndOfPattern() || node.getChildren().containsKey("#");
        }

        String part = parts[index];

        // Match direct child
        if (node.getChildren().containsKey(part) && match(parts, index + 1, node.getChildren().get(part))) {
            return true;
        }

        if (node.getChildren().containsKey("*") && match(parts, index + 1, node.getChildren().get("*"))) {
            return true;
        }

        if (node.getChildren().containsKey("#") ) {
            if (node.getChildren().get("#").isEndOfPattern()){
                return true;
            }
            else if (match(parts, index + 1, node.getChildren().get("#"))) {
                return true;
            } else {
                boolean result = false;
                for (int i = index; i < parts.length; i++) {
                    result = match(parts, i, node.getChildren().get("#"));
                    if(result)
                        return result;
                }
            }
        }

        return false;
    }




}
