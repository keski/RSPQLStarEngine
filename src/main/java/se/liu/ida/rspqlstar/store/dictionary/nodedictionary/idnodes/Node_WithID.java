package se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes;

import org.apache.jena.graph.Node;

public interface Node_WithID {
    long getId();

    Node asJenaNode();
}
