package se.liu.ida.rspqlstar.store.dataset;

import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import se.liu.ida.rspqlstar.lang.NamedWindow;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.*;

/**
 * StreamingDatasetGraph it a wrapper for all datasets to be queried. The execution time is set
 * and it remains fixed static throughout each execution.
 */
public class StreamingDatasetGraph extends AbstractDatasetGraph {
    private DatasetGraphStar baseDataset = new DatasetGraphStar();
    private Map<String, WindowDatasetGraph> windows = new HashMap<>();
    private Map<String, RDFStream> rdfStreams = new HashMap<>();
    private DatasetGraphStar activeDataset = baseDataset;
    private Date time = new Date();
    private boolean ready = false;

    /**
     * Create a StreamingDatasetGraph.
     */
    public StreamingDatasetGraph(){}

    /**
     * Create a StreamingDatasetGraph from a query and a set of RDFStreams. The constructor
     * creates a WindowDatasetGraph for each named window mentioned in the query.
     */

    public void registerStream(RDFStream rdfStream){
        rdfStreams.put(rdfStream.iri, rdfStream);
    }

    /**
     * Initialize the dataset for use with a given query. All streams stream on which the
     * the query is dependent must be registered.
     *
     * Note: A single dataset can be used for multiple parallel queries; however, each named
     * window must be unique, since  aA WindowDatasetGraph is created for each named window
     * mentioned in the query.
     */

    public void initForQuery(RSPQLStarQuery query){
        for(NamedWindow w : query.getNamedWindows().values()){
            final String name = w.getWindowName();
            final Duration range = w.getRange();
            final Duration step = w.getStep();
            final RDFStream rdfStream = rdfStreams.get(w.getStreamName());
            if(rdfStream == null){
                throw new IllegalStateException("The RDFStream " + w.getStreamName() + " has not been registered");
            }
            addWindow(new WindowDatasetGraph(name, range, step, time, rdfStream));
        }
        ready = true;
    }

    public void setBaseDataset(DatasetGraphStar dataset){
        baseDataset = dataset;
    }

    public DatasetGraphStar getBaseDataset(){
        return baseDataset;
    }

    public void addWindow(WindowDatasetGraph window){
        windows.put(window.name, window);
    }

    public DatasetGraphStar getActiveDataset(){
        return activeDataset;
    }

    public void setTime(Date time){
        this.time = time;
        //windows.values().forEach(w -> w.getDataset(time.getTime()));
    }

    public Date getTime(){
        return time;
    }

    @Override
    public Graph getDefaultGraph() {
        return activeDataset.getDefaultGraph();
    }

    @Override
    public void add(Quad quad){
        activeDataset.add(quad);
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

    /**
     * Iterate all quads in all datasets associated with this StreamingDatasetGraph.
     *
     * @return
     */
    public Iterator<IdBasedQuad> iterator(){
        final IteratorChain<IdBasedQuad> iteratorChain = new IteratorChain<>();
        iteratorChain.addIterator(baseDataset.iterateAll());
        windows.forEach((iri, w) -> iteratorChain.addIterator(w.iterate(time)));
        return iteratorChain;
    }

    public void useWindowDataset(String name) {
        final WindowDatasetGraph dsg = windows.get(name);
        if(dsg == null){
            throw new IllegalStateException("The named window " + name + " does not exist.");
        }
        activeDataset = dsg.getDataset(time.getTime());
    }

    public void useBaseDataset() {
        activeDataset = baseDataset;
    }

    public DatasetGraphStar getWindowDataset(String name) {
        final WindowDatasetGraph dsg = windows.get(name);
        if(dsg == null){
            throw new IllegalStateException("The named window " + name + " does not exist.");
        }
        return dsg.getDataset(time.getTime());
    }

    public boolean isReady() {
        return ready;
    }
}
