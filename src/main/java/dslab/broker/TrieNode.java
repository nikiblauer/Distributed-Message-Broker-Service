package dslab.broker;

import java.util.HashMap;
import java.util.Map;

class TrieNode {
    private final Map<String, TrieNode> children;
    private boolean isEndOfPattern;  // Marks the end of a binding pattern

    public TrieNode() {
        children = new HashMap<>();
        isEndOfPattern = false;
    }

    // Get or create a child node for a specific part (word) or wildcard
    TrieNode getOrCreateChild(String part) {
        return children.computeIfAbsent(part, k -> new TrieNode());
    }

    public Map<String, TrieNode> getChildren() {
        return children;
    }

    public void setIsEndOfPattern(boolean endOfPattern) {
        isEndOfPattern = endOfPattern;
    }

    public boolean isEndOfPattern() {
        return isEndOfPattern;
    }
}
