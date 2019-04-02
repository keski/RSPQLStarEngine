package se.liu.ida.rspqlstar.store.dataset;

import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;

import java.util.Iterator;

/**
 * The WindowDatasetGraph generates a dataset from an underlying stream. The class implements a basic
 * locking mechanism.
 */

public class WindowDatasetGraph extends DatasetGraphStar {
    public String iri;
    public long width;
    public long step;
    public long startTime;
    public RDFStream rdfStream;

    public long cachedUpperBound = -1;
    public DatasetGraphStar cachedDatasetGraph;

    public WindowDatasetGraph(String name, long width, long step, long startTime, RDFStream rdfStream){
        this.iri = name;
        this.width = width;
        this.step = step;
        this.startTime = startTime;
        this.rdfStream = rdfStream;
    }

    public DatasetGraphStar getDataset(long time){
        final long upperBound = getUpperBound(time);
        // use cached dataset
        if(cachedUpperBound == upperBound) {
            return cachedDatasetGraph;
        }

        final DatasetGraphStar ds = new DatasetGraphStar();
        rdfStream.iterator(upperBound - width, upperBound)
                .forEachRemaining(ds::addToIndex);
        cachedDatasetGraph = ds;
        cachedUpperBound = upperBound;
        return cachedDatasetGraph;
    }

    private long getUpperBound(long time){
        long n = (time - (startTime + width))/step;
        return startTime + width + n * step;
    }

    public Iterator<IdBasedQuad> iterateAll(long time){
        return getDataset(time).iterateAll();
    }
}
