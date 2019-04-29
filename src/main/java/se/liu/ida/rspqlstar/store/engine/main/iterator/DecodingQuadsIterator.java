package se.liu.ida.rspqlstar.store.engine.main.iterator;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadStarPattern;

import java.util.Iterator;

/**
 * Class for making a Jena ExtendedIterator.
 */
public class DecodingQuadsIterator extends NiceIterator<Quad> implements ExtendedIterator<Quad> {
    final private NodeDictionary nd = NodeDictionaryFactory.get();
    final private Iterator<IdBasedQuad> inputIterator;
    //final private QuadStarPattern pattern;

    public DecodingQuadsIterator(Iterator<IdBasedQuad> inputIterator) {
        this.inputIterator = inputIterator;
    }

    public DecodingQuadsIterator(Iterator<IdBasedQuad> inputIterator, QuadStarPattern pattern) {
        //this.pattern = pattern;
        this.inputIterator = inputIterator;
    }

    @Override
    final public boolean hasNext() {
        return inputIterator.hasNext();
    }

    @Override
    final public Quad next() {
        return decode(inputIterator.next());
    }

    @Override
    final public void remove() {
        inputIterator.remove();
    }

    /**
     * Transform an IdBasedQuad into a Jena quad.
     *
     * @param idBasedQuad
     * @return
     */
    public Quad decode(IdBasedQuad idBasedQuad) {
        final Node graph = nd.getNode(idBasedQuad.graph);
        final Node subject = nd.getNode(idBasedQuad.subject);
        final Node predicate = nd.getNode(idBasedQuad.predicate);
        final Node object = nd.getNode(idBasedQuad.object);

        return Quad.create(graph, subject, predicate, object);
    }
}

