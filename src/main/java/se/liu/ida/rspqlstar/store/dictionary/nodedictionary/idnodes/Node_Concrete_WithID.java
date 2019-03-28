package se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.graph.NodeVisitor;
import org.apache.jena.graph.Node_Concrete;

/**
 * Node implementation that supports IDs. Note that the Jena classes corresponding to
 * to literal, URI, and blank node cannot be extended since they have protected constructors.
 */

public abstract class Node_Concrete_WithID extends Node_Concrete implements Node_WithID {
    final private long id;

    protected Node_Concrete_WithID(Object label, long id) {
        super(label);
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public Object visitWith(NodeVisitor v) {
        throw new NotImplementedException("visitWith not implemented");
    }

    @Override
    public int hashCode() {
        return label.hashCode() * 31;
    }

}
