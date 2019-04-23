package se.liu.ida.rspqlstar.store.engine.main.iterator;

import org.apache.jena.graph.Node;
import se.liu.ida.rspqlstar.store.engine.main.quadpattern.Key;

public class NodeWrapperKey extends Key {
    final public Node node;

    public NodeWrapperKey(Node node) {
        super(0L); // any value
        this.node = node;
    }

    public String toString(){
        return String.format("NodeWrapperKey(%s)", node);
    }
}
