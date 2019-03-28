package se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.shared.PrefixMapping;

/**
 * This class is purely meant to be used as a placeholder node, e.g. to provide reserved
 * IDs with a corresponding node.
 */
public class Node_Placeholder extends Node_Concrete_WithID implements Node_WithID {
    long id;
    String label;

    public Node_Placeholder(String label, long id){
        super(label, id);
        this.id = id;
        this.label = label;
    }

    @Override
    public boolean equals(Object o) {
        return o == this;
    }

    @Override
    public String toString(){
        return label;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public Node asJenaNode() {
        throw new IllegalStateException("Node_Placeholder should not be treated as regular node.");
    }
}
