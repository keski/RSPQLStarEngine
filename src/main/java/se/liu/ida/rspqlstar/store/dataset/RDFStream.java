package se.liu.ida.rspqlstar.store.dataset;

import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RDFStream {
    final public String iri;
    final private List<TimestampedGraph> timestampedGraphs = new ArrayList<>();

    public RDFStream(String iri){
        this.iri = iri;
    }

    public void push(TimestampedGraph timestampedGraph){
        timestampedGraphs.add(timestampedGraph);
    }

    public Iterator<IdBasedQuad> iterator(long lowerBound, long upperBound){
        final IteratorChain<IdBasedQuad> iteratorChain = new IteratorChain<>();
        for(int i=0; i < timestampedGraphs.size(); i++){
            final TimestampedGraph tg = timestampedGraphs.get(i);
            if(lowerBound > tg.time) continue;
            if(upperBound <= tg.time) break;
            iteratorChain.addIterator(tg.dgs.iterateAll());
        }
        return iteratorChain;
    }

    public List<TimestampedGraph> iterateElements(long lowerBound, long upperBound){
        final List<TimestampedGraph> tgs = new ArrayList<>();
        for(TimestampedGraph tg : timestampedGraphs){
            if(lowerBound > tg.time) continue;
            if(upperBound <= tg.time) break;
            tgs.add(tg);
        }
        return tgs;
    }
}
