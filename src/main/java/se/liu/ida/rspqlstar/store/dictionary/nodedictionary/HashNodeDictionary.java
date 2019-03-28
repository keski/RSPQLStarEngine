package se.liu.ida.rspqlstar.store.dictionary.nodedictionary;

import java.util.*;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.Node_WithID;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.NodeWithIDFactory;
import se.liu.ida.rspqlstar.store.triple.IdFactory;
import org.apache.jena.graph.Node;
import se.liu.ida.rspqlstar.store.utils.Configuration;

/**
 * The node dictionary keeps track of the mappings between nodes and their respective IDs.
 */

public class HashNodeDictionary implements NodeDictionary {
    final private ArrayList<Node> idToNode = new ArrayList<>();
    final private Map<Node, Long> nodeToId = new Object2LongOpenHashMap<>();

    public HashNodeDictionary() {
        super();
    }

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

        final long newId = IdFactory.nextNodeId();
        final Node n = NodeWithIDFactory.createNode(node, newId);
        return addNode(n, newId);
    }

    @Override
    public long addNode(Node node, long id){
        if (id < idToNode.size()) {
            idToNode.set((int) id, node); // replace existing value
        } else {
            idToNode.add(node);
        }
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
    public void print(int limit) {
        StringBuilder sb = new StringBuilder();
        sb.append("Node Dictionary\n");

        for (int id = 0; id < idToNode.size() && id < limit; id++) {
            Node node = getNode(id);
            sb.append(id + " : " + node + "\n");
        }
        System.out.println(sb.toString());
    }
}
