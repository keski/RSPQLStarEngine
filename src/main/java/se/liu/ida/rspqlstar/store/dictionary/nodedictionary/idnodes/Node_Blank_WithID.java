package se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes;

import org.apache.jena.graph.BlankNodeId;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

public class Node_Blank_WithID extends Node_Concrete_WithID {

    public Node_Blank_WithID(BlankNodeId blankNodeId, long id) {
        super(blankNodeId, id);
    }

    @Override
    public boolean isBlank() {
        return true;
    }

    @Override
    public BlankNodeId getBlankNodeId() {
        return (BlankNodeId) label;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other instanceof Node_Blank_WithID) {
            return label.equals(((Node_Blank_WithID) other).label);
        } else {
            // Warning: Not symmetric.
            return asJenaNode().equals(other);
        }
    }

    @Override
    public Node asJenaNode() {
        return NodeFactory.createBlankNode(getBlankNodeId());
    }
}
