package se.liu.ida.rspqlstar.store.dataset;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;


public class TimestampedGraph implements StreamRDF {
    public DatasetGraphStar datasetGraph = new DatasetGraphStar();
    public long time;

    public TimestampedGraph(long time){
        this.time = time;
    }

    @Override
    public void start() {
        datasetGraph = new DatasetGraphStar(); // override this to provide a more streamlined implementation
    }

    @Override
    public void triple(Triple triple) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void quad(Quad quad) {
        datasetGraph.add(quad);
    }

    @Override
    public void base(String s) {}

    @Override
    public void prefix(String s, String s1) {}

    @Override
    public void finish() {}
}
