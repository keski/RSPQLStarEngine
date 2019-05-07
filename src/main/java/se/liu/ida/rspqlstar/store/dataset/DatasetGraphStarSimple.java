package se.liu.ida.rspqlstar.store.dataset;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionary;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.main.iterator.DecodingQuadsIterator;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadPatternBuilder;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.index.Field;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;
import se.liu.ida.rspqlstar.store.index.Index;
import se.liu.ida.rspqlstar.store.index.TreeIndex;

import java.util.Collections;
import java.util.Iterator;

/**
 * The DatasetStarGraph does not contain any data in itself. Instead, it leverages the
 * indexes in the QuadStore and exposes the data as if it was represented
 * using the Jena API classes using on-the-fly decoding.
 */

public class DatasetGraphStarSimple extends AbstractDatasetGraph {
    final private Logger logger = Logger.getLogger(DatasetGraphStarSimple.class);
    final public Index GSPO;
    final NodeDictionary nd = NodeDictionaryFactory.get();
    final ReferenceDictionary refT = ReferenceDictionaryFactory.get();

    public DatasetGraphStarSimple() {
        GSPO = new TreeIndex(Field.G, Field.S, Field.P, Field.O);
    }

    public boolean contains(QuadStarPattern pattern) {
        return GSPO.contains(pattern);
    }

    public boolean contains(IdBasedQuad idBasedQuad) {
        return GSPO.contains(idBasedQuad);
    }

    @Override
    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        throw new IllegalStateException("Illegal operation");
    }

    @Override
    public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
        throw new IllegalStateException("Illegal operation");
    }


    @Override
    public Graph getDefaultGraph() {
        logger.debug("Accessing default graph of DatasetGraphStar, was this intentional? Returning empty graph.");
        return ModelFactory.createDefaultModel().getGraph();
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
        if (subject instanceof Node_Triple) {
            final Quad q = new Quad(graph, ((Node_Triple) subject).get());
            IdBasedQuad idBasedQuad = addQuad(q);
            s = refT.addIfNecessary(idBasedQuad.getIdBasedTriple());
        } else {
            s = nd.addNodeIfNecessary(subject);
        }

        final long o;
        if (object instanceof Node_Triple) {
            IdBasedQuad idBasedQuad = addQuad(new Quad(graph, ((Node_Triple) object).get()));
            o = refT.addIfNecessary(idBasedQuad.getIdBasedTriple());
        } else {
            o = nd.addNodeIfNecessary(object);
        }

        final IdBasedQuad idBasedQuad = new IdBasedQuad(g, s, p, o);
        addToIndex(idBasedQuad);
        return idBasedQuad;
    }

    public void addToIndex(IdBasedQuad idBasedQuad) {
        GSPO.add(idBasedQuad);
    }

    public Iterator<IdBasedQuad> iterateAll() {
        return GSPO.iterateAll();
    }

}
