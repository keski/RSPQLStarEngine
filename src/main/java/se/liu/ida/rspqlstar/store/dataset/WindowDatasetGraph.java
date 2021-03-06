package se.liu.ida.rspqlstar.store.dataset;

import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;

import java.time.Duration;
import java.util.Date;
import java.util.Iterator;

/**
 * The WindowDatasetGraph generates a dataset from an underlying stream. The class implements a basic
 * locking mechanism.
 */

public class WindowDatasetGraph extends DatasetGraphStar {
    public Logger logger = Logger.getLogger(WindowDatasetGraph.class);
    public String name;
    public long width;
    public long step;
    public long startTime;
    public RDFStarStream rdfStream;

    public long cachedUpperBound = -1;
    public DatasetGraphStar cachedDatasetGraph;

    public WindowDatasetGraph(String name, Duration width, Duration step, Date startTime, RDFStarStream rdfStream){
        this.name = name;
        this.width = width.toMillis();
        this.step = step.toMillis();
        this.startTime = startTime.getTime();
        this.rdfStream = rdfStream;
    }

    public DatasetGraphStar getDataset(long time){
        final long upperBound = getUpperBound(time);
        // use cached dataset
        if(cachedUpperBound == upperBound) {
            logger.debug("Using cached dataset for window: " + name);
            return cachedDatasetGraph;
        }
        logger.debug("Not using cached dataset for window: " + name);

        final DatasetGraphStar ds = new DatasetGraphStar();
        rdfStream.iterator(upperBound - width, upperBound).forEachRemaining(ds::addToIndex);
        cachedDatasetGraph = ds;
        cachedUpperBound = upperBound;

        return cachedDatasetGraph;
    }

    private long getUpperBound(long time){
        long n = (time - (startTime + width))/step;
        return startTime + width + n * step;
    }

    public Iterator<IdBasedQuad> iterate(Date time){
        return getDataset(time.getTime()).iterateAll();
    }

    public String toString(){
        return String.format("WindowDatasetGraph(size: %s)", GSPO.size());
    }
}
