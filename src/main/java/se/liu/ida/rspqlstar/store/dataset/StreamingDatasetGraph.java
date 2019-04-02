package se.liu.ida.rspqlstar.store.dataset;

import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * For each query, a StreamingDatasetGraph is defined. Before query evaluation takes place the execution time
 * is set and it remains fixed static throughout the entire evaluation
 */
public class StreamingDatasetGraph extends AbstractDatasetGraph {
    private DatasetGraphStar baseDataset = new DatasetGraphStar();
    private Map<String, WindowDatasetGraph> windows = new HashMap<>();
    private DatasetGraphStar activeDataset = baseDataset;
    private long time = -1;

    public void setBaseDataset(DatasetGraphStar dataset){
        baseDataset = dataset;
    }

    public DatasetGraphStar getBaseDataset(){
        return baseDataset;
    }

    public void addWindow(WindowDatasetGraph window){
        windows.put(window.iri, window);
    }

    public DatasetGraphStar getActiveDataset(){
        return activeDataset;
    }

    public void setActiveDataset(DatasetGraphStar dataset){
        activeDataset = dataset;
    }

    public WindowDatasetGraph getWindow(String iri){
        return windows.get(iri);
    }

    public void setTime(long time){
        this.time = time;
    }

    @Override
    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        return getActiveDataset().find(g, s, p , o);
    }

    @Override
    public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
        if(g.equals(Quad.defaultGraphIRI)){
            return null;
        }
        return getActiveDataset().find(g, s, p , o);
    }

    public Iterator<IdBasedQuad> iterator(){
        final IteratorChain<IdBasedQuad> iteratorChain = new IteratorChain<>();
        iteratorChain.addIterator(baseDataset.iterateAll());
        windows.forEach((iri, w) -> iteratorChain.addIterator(w.iterateAll(time)));
        return iteratorChain;
    }
}
