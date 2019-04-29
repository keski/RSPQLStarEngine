package se.liu.ida.rspqlstar.store.dictionary.nodedictionary;

import org.apache.jena.graph.Node;

import java.io.PrintStream;

public interface NodeDictionary {
    Node getNode(long id);

    long addNodeIfNecessary(Node node);

    long addNode(Node node, long id);

    Long getId(Node node);

    long size();

    void print(PrintStream out, int limit);
}
