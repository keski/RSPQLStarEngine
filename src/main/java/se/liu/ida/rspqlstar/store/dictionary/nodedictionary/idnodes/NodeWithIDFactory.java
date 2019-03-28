package se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes;

import org.apache.jena.graph.*;
import org.apache.jena.graph.impl.LiteralLabel;
import se.liu.ida.rspqlstar.store.utils.Configuration;

public class NodeWithIDFactory {

    public static Node createNode(Node node, long id) {
        final Node n;

        if (node instanceof Node_Blank) {
            n = new Node_Blank_WithID(node.getBlankNodeId(), id);
        } else if (node instanceof Node_URI) {
            n = new Node_URI_WithID(node.getURI(), id);
        } else if (node instanceof Node_Literal) {
            final LiteralLabel l = node.getLiteral();
            n = new Node_Literal_WithID(l, id);
        } else if (node instanceof Node_Triple) {
            // s, p, o keys
            n = new Node_Triple_WithID(((Node_Triple) node).get(), id);
        } else {
            throw new IllegalStateException("Failed to create node with id for node: " + node);
        }
        return n;
    }

    /**
     * Make NodeWithId from a Jena node with a placeholder ID. This is used when looking up keys from a Jena node in
     * the node dictionary.
     *
     * @param node
     * @return
     */
    public static Node createNode(Node node) {
        return createNode(node, 0L);
    }
}
