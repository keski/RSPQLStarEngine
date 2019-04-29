package se.liu.ida.rspqlstar.store.dictionary.nodedictionary;

import org.apache.jena.graph.Node;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.NodeWithIDFactory;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.Node_WithID;
import se.liu.ida.rspqlstar.store.dictionary.IdFactory;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * The node dictionary keeps track of the mappings between nodes and their respective IDs.
 */

public class HashNodeDictionary implements NodeDictionary {
    final private List<Node> idToNode =  new ArrayList<>();
    final private ConcurrentHashMap<Node, Long> nodeToId = new ConcurrentHashMap<>();

    @Override
    public Node getNode(long id) {
        return idToNode.get((int) id);
    }

    @Override
    public long addNodeIfNecessary(Node node) {
        final Long id = getId(node);
        if (id != null) {
            return id;
        }

        long newId = IdFactory.nextNodeId();
        Node n = NodeWithIDFactory.createNode(node, newId);
        return addNode(n, newId);
    }

    @Override
    public long addNode(Node node, long id){
        idToNode.add(node);
        nodeToId.put(node, id);
        return id;
    }

    @Override
    public Long getId(Node node) {
        if (node instanceof Node_WithID) {
            return ((Node_WithID) node).getId();
        } else {
            return nodeToId.get(NodeWithIDFactory.createNode(node));
        }
    }

    @Override
    public long size() {
        return nodeToId.size();
    }

    @Override
    public void print(PrintStream out, int limit) {
        StringBuilder sb = new StringBuilder();
        sb.append("Node Dictionary\n");

        for (int id = 0; id < idToNode.size() && id < limit; id++) {
            Node node = getNode(id);
            sb.append(id + " : " + node + "\n");
        }
        out.println(sb.toString());
    }
}
