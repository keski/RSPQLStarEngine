package se.liu.ida.rspqlstar.store.dictionary.nodedictionary;

import org.apache.jena.graph.Node;

/**
 * An implementation of {@link NodeDictionary} that adds a small
 * cache in front of another {@link NodeDictionary}.
 * 
 * Such a cache may improve query performance for the following
 * reason: After evaluating a BGP using the ID-based iterators,
 * the {@link DecodeBindingsIterator} turns the resulting ID-based
 * solution mappings into Jena Binding objects (which are Jena
 * Node-based solution mappings). To this end, the iterator has
 * to access the node dictionary, which it does for every solution
 * mapping separately. Now, for many queries, there may be multiple
 * solution mappings that all have the same bindings for a subset of
 * their variables (e.g., m1={?x->21,?y->34} and m2={?x->21,?y->9}).
 * In these cases , a small cache in front of the node dictionary may
 * come in handy.
 * 
 * The cache replacement is implemented using the clock algorithm.
 * 
 * TODO: Currently, this class only adds a cache for key lookups.
 * For queries that contain multiple BGPs, it may be beneficial to
 * also have a cache for node lookups.
 */
public class CachingNodeDictionary extends NodeDictionaryWrapper
{
	static public final int CACHE_SIZE = 10;

	final protected Long[]    cachedKeysForKeyLookups     = new Long  [CACHE_SIZE];
	final protected Node[]    cachedNodesForKeyLookups    = new Node   [CACHE_SIZE];
	final protected boolean[] referencedBitsForKeyLookups = new boolean[CACHE_SIZE];
	protected int  pointerForKeyLookups        = 0;
	protected long numberOfHitsForKeyLookups   = 0L;
	protected long numberOfMissesForKeyLookups = 0L;

	public CachingNodeDictionary( NodeDictionary wrappedDict ) { super(wrappedDict); }

	@Override
	public Node getNode( long id )
	{
		// check the cache
		for ( int i = 0; i < CACHE_SIZE; i++ )
		{
			if ( cachedKeysForKeyLookups[i] == null ) {
				break;
			}
			if ( cachedKeysForKeyLookups[i].equals(id) ) {
				numberOfHitsForKeyLookups++;
				referencedBitsForKeyLookups[i] = true;
				return cachedNodesForKeyLookups[i];
			}
		}

		// Cache miss. We have to resort to the node dictionary.
		numberOfMissesForKeyLookups++;
		final Node node = super.getNode(id);

		// Before returning the resulting Node, add it to the
		// cache. To this end, find a cache entry to be replaced.
		while ( referencedBitsForKeyLookups[pointerForKeyLookups] == true ) {
			referencedBitsForKeyLookups[pointerForKeyLookups] = false;
			advancePointerForKeyLookups();
		}

		// Replace the identified cache entry.
		cachedKeysForKeyLookups[pointerForKeyLookups]     = id;
		cachedNodesForKeyLookups[pointerForKeyLookups]    = node;
		referencedBitsForKeyLookups[pointerForKeyLookups] = true;
		advancePointerForKeyLookups();

		return node;
	}

	protected void advancePointerForKeyLookups()
	{
		pointerForKeyLookups++;
		if ( pointerForKeyLookups == CACHE_SIZE )
			pointerForKeyLookups = 0;
	}

	public long getNumberOfHitsForKeyLookups() { return numberOfHitsForKeyLookups; }

	public long getNumberOfMissesForKeyLookups() { return numberOfMissesForKeyLookups; }

	protected void clearCacheForKeyLookups()
	{
		for ( int i = 0; i < CACHE_SIZE; i++ ) {
			cachedKeysForKeyLookups[i]     = null;
			cachedNodesForKeyLookups[i]    = null;
			referencedBitsForKeyLookups[i] = false;
		}

		pointerForKeyLookups = 0;
		numberOfHitsForKeyLookups   = 0L;
		numberOfMissesForKeyLookups = 0L;
	}

}
