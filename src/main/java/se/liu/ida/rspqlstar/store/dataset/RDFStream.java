package se.liu.ida.rspqlstar.store.dataset;

import org.apache.commons.collections4.iterators.IteratorChain;
import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RDFStream {
    final private String iri;
    final private ConcurrentLinkedQueue<TimestampedGraph> elements;

    public RDFStream(String iri){
        this.iri = iri;
        elements = new ConcurrentLinkedQueue<>();
    }

    public void add(TimestampedGraph timestampedGraph){
        elements.add(timestampedGraph);
    }

    public Iterator<IdBasedQuad> iterator(long lowerBound, long upperBound){
        final IteratorChain<IdBasedQuad> iteratorChain = new IteratorChain<>();
        elements.forEach(tg -> {
            if(lowerBound <= tg.time && tg.time < upperBound){
                iteratorChain.addIterator(tg.datasetGraph.iterateAll());
            }
        });
        return iteratorChain;
    }

}
