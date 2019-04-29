package se.liu.ida.rspqlstar.store.dictionary.nodedictionary;

import org.apache.jena.graph.Node;

import java.io.PrintStream;

/**
 * An implementation of {@link NodeDictionary} that simply wraps another
 * {@link NodeDictionary}. Subclasses may override some of the functionality.
 *
 */
public class NodeDictionaryWrapper implements NodeDictionary {
	protected final NodeDictionary wrappedDict;

	public NodeDictionaryWrapper( NodeDictionary wrappedDict ) { this.wrappedDict = wrappedDict; }

	@Override
	public Node getNode( long id ) { return wrappedDict.getNode(id); }

	@Override
    public long addNodeIfNecessary( Node node ) { return wrappedDict.addNodeIfNecessary(node); }

	@Override
	public long addNode( Node node, long id ) { return wrappedDict.addNode(node, id); }


	@Override
    public Long getId( Node node ) { return wrappedDict.getId(node); }

	@Override
    public long size() { return wrappedDict.size(); }

	@Override
	public void print(PrintStream out, int limit) {
		wrappedDict.print(out, limit);
	}

}
