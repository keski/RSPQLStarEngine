package se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.shared.PrefixMapping;

public class Node_URI_WithID extends Node_Concrete_WithID {
    public Node_URI_WithID(String label, long id) {
        super(label, id);
    }

    @Override
    public String getURI() {
        return (String) label;
    }

    @Override
    public boolean isURI() {
        return true;
    }

    @Override
    public String toString(PrefixMapping pm, boolean quoting) {
        return pm == null ? (String) label : pm.shortForm((String) label);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other instanceof Node_URI_WithID) {
            return label.equals(((Node_URI_WithID) other).getURI());
        } else {
            // Warning: Not symmetric.
            return asJenaNode().equals(other);
        }
    }

    @Override
    public String getNameSpace() {
        final String s = (String) label;
        return s.substring(0, Util.splitNamespaceXML(s));
    }

    @Override
    public String getLocalName() {
        final String s = (String) label;
        return s.substring(Util.splitNamespaceXML(s));
    }

    @Override
    public boolean hasURI(String uri) {
        return label.equals(uri);
    }


    @Override
    public Node asJenaNode() {
        return NodeFactory.createURI(getURI());
    }
}
