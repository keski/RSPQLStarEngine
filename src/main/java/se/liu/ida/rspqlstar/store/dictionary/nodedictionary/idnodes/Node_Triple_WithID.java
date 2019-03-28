package se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;

public class Node_Triple_WithID extends Node_Triple implements Node_WithID {
    final private long id;

    public Node_Triple_WithID(Triple triple, long id) {
        super(triple);
        this.id = id;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other instanceof Node_WithID) {
            return getId() == ((Node_WithID) other).getId();
        } else {
            return super.equals(other);
        }
    }

    @Override
    public long getId() {
        return id;
    }


    /**
     * Is this an artefact of mixing up the two worlds?
     *
     * @return
     */
    public Node_Triple asJenaNode() {
        final Triple t = get();
        final Node s, p, o;
        if (t.getSubject() instanceof Node_WithID) {
            s = ((Node_WithID) t.getSubject()).asJenaNode();
        } else {
            s = t.getSubject();
        }

        if (t.getSubject() instanceof Node_WithID) {
            p = ((Node_WithID) t.getPredicate()).asJenaNode();
        } else {
            p = t.getSubject();
        }

        if (t.getSubject() instanceof Node_WithID) {
            o = ((Node_WithID) t.getObject()).asJenaNode();
        } else {
            o = t.getSubject();
        }
        return new Node_Triple(new Triple(s, p, o));
    }
}
