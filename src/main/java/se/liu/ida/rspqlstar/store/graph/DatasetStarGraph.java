package se.liu.ida.rspqlstar.store.graph;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionary;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;
import se.liu.ida.rspqlstar.store.triplepattern.QuadPatternBuilder;
import se.liu.ida.rspqlstar.store.triplepattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.triplestore.QuadStore;

import java.util.Collections;
import java.util.Iterator;

/**
 * The DatasetStarGraph does not contain any data in itself. Instead, it leverages the
 * indexes in the QuadStore and exposes the data as if it was represented
 * using the Jena API classes using on-the-fly decoding.
 */

public class DatasetStarGraph extends AbstractDatasetStarGraph {
    private QuadStore store = new QuadStore();
    private final NodeDictionary nd = NodeDictionaryFactory.get();
    private final ReferenceDictionary rd = ReferenceDictionaryFactory.get();

    public QuadStore getStore(){
        return store;
    }

    public boolean contains(QuadStarPattern quad) {
        return find(quad).hasNext();
    }

    @Override
    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        QuadStarPattern quadPattern = getQuadPattern(g, s, p, o);
        return new DecodingQuadsIterator(store.find(quadPattern), quadPattern);
    }

    public Iterator<IdBasedQuad> find(QuadStarPattern quadPattern) {
        return store.find(quadPattern);
    }

    private QuadStarPattern getQuadPattern(Node g, Node s, Node p, Node o) {
        QuadPatternBuilder builder = new QuadPatternBuilder();
        builder.setGraph(g);
        builder.setSubject(s);
        builder.setPredicate(p);
        builder.setObject(o);
        return builder.createQuadPattern();
    }

    @Override
    public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
        // Only check named graphs
        if(g == null){
            return Collections.emptyIterator();
        }
        return find(g, s, p, o);
    }

}
