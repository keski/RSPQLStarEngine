package se.liu.ida.rspqlstar.store.dataset;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.sparql.core.Quad;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionary;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.DecodingQuadsIterator;
import se.liu.ida.rspqlstar.store.queryengine.FilteredTripleIterator;
import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;
import se.liu.ida.rspqlstar.store.triplepattern.QuadPatternBuilder;
import se.liu.ida.rspqlstar.store.triplepattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.triplestore.Field;
import se.liu.ida.rspqlstar.store.triplestore.Index;
import se.liu.ida.rspqlstar.store.triplestore.treeindex.TreeIndex;

import java.util.Collections;
import java.util.Iterator;

/**
 * The DatasetStarGraph does not contain any data in itself. Instead, it leverages the
 * indexes in the QuadStore and exposes the data as if it was represented
 * using the Jena API classes using on-the-fly decoding.
 */

public class DatasetGraphStar extends AbstractDatasetGraph { //implements DatasetGraph {
    final private Index GSPO;
    final private Index GPOS;
    final private Index GOSP;
    final private Index SPOG;
    final private Index POSG;
    final private Index OSPG;
    final NodeDictionary nd = NodeDictionaryFactory.get();
    final ReferenceDictionary refT = ReferenceDictionaryFactory.get();


    Graph graphWrapper = new GraphMem(); // This is just a dummy... How do we get rid of it?

    public DatasetGraphStar(){
        GSPO = new TreeIndex(Field.G, Field.S, Field.P, Field.O);
        GPOS = new TreeIndex(Field.G, Field.P, Field.O, Field.S);
        GOSP = new TreeIndex(Field.G, Field.O, Field.S, Field.P);
        SPOG = new TreeIndex(Field.S, Field.P, Field.O, Field.G);
        POSG = new TreeIndex(Field.P, Field.O, Field.S, Field.G);
        OSPG = new TreeIndex(Field.O, Field.S, Field.P, Field.G);
    }

    public boolean contains(QuadStarPattern quad) {
        return find(quad).hasNext();
    }

    @Override
    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        QuadStarPattern quadPattern = getQuadPattern(g, s, p, o);
        return new DecodingQuadsIterator(find(quadPattern), quadPattern);
    }

    @Override
    public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
        return find(g, s, p, o);
    }


    @Override
    public Graph getDefaultGraph() {
        return graphWrapper;
    }

    @Override
    public void add(Quad quad) {
        addQuad(quad);
    }

    private IdBasedQuad addQuad(Quad quad) {
        final Node graph = quad.getGraph();
        final Node subject = quad.getSubject();
        final Node predicate = quad.getPredicate();
        final Node object = quad.getObject();

        final long g = nd.addNodeIfNecessary(graph);
        final long p = nd.addNodeIfNecessary(predicate);

        final long s;
        if(subject instanceof Node_Triple){
            final Quad q = new Quad(graph, ((Node_Triple) subject).get());
            IdBasedQuad idBasedQuad = addQuad(q);
            s = refT.addIfNecessary(idBasedQuad);
        } else {
            s = nd.addNodeIfNecessary(subject);
        }

        final long o;
        if(object instanceof Node_Triple){
            IdBasedQuad idBasedQuad = addQuad(new Quad(graph, ((Node_Triple) object).get()));
            o= refT.addIfNecessary(idBasedQuad);
        } else {
            o = nd.addNodeIfNecessary(object);
        }

        final IdBasedQuad idBasedQuad = new IdBasedQuad(g, s, p, o);
        addToIndex(idBasedQuad);
        return idBasedQuad;
    }

    public void addToIndex(IdBasedQuad idBasedQuad) {
        GSPO.add(idBasedQuad);
        GPOS.add(idBasedQuad);
        GOSP.add(idBasedQuad);
        SPOG.add(idBasedQuad);
        POSG.add(idBasedQuad);
        OSPG.add(idBasedQuad);
    }

    public Iterator<IdBasedQuad> iterateAll() {
        return GSPO.iterateAll();
    }

    /**
     * Identifies the correct index to query and returns an iterator over the quad pattern.
     * @param pattern
     * @return
     */
    public Iterator<IdBasedQuad> find(QuadStarPattern pattern) {
        if (!pattern.isMatchable()) return Collections.emptyIterator();

        final boolean g = pattern.graph.isConcrete();
        final boolean s = pattern.subject.isConcrete();
        final boolean p = pattern.predicate.isConcrete();
        final boolean o = pattern.object.isConcrete();

        // This order is not based on anything in particular
        // GSP, GOS, GPO, GS, GO, GP, G,
        // SP, OS, PO, S, O, P

        final Iterator<IdBasedQuad> iter;
        if (g && s && p) iter = GSPO.iterator(pattern);
        else if(g && o && s) iter = GOSP.iterator(pattern);
        else if(g && p && o) iter = GPOS.iterator(pattern);
        else if(g && s) iter = GSPO.iterator(pattern);
        else if(g && o) iter = GOSP.iterator(pattern);
        else if(g && p) iter = GPOS.iterator(pattern);
        else if(g) iter = GSPO.iterator(pattern); // its probably less selective than this...
        else if (s && p && o) iter = SPOG.iterator(pattern);
        else if(o && s) iter = OSPG.iterator(pattern);
        else if(p && o) iter = POSG.iterator(pattern);
        else if(s) iter = SPOG.iterator(pattern);
        else if(o) iter = OSPG.iterator(pattern);
        else if(p) iter = POSG.iterator(pattern);
        else iter = GSPO.iterateAll();

        return new FilteredTripleIterator(iter, pattern);
    }

    private QuadStarPattern getQuadPattern(Node g, Node s, Node p, Node o) {
        QuadPatternBuilder builder = new QuadPatternBuilder();
        builder.setGraph(g);
        builder.setSubject(s);
        builder.setPredicate(p);
        builder.setObject(o);
        return builder.createQuadPattern();
    }

}
