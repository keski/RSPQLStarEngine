package se.liu.ida.rspqlstar.stream;

import se.liu.ida.rspqlstar.store.dataset.RDFStream;
import se.liu.ida.rspqlstar.store.dataset.TimestampedGraph;

import java.text.SimpleDateFormat;

public abstract class RSPQLStarStream implements Runnable {
    static public final String BASE = "http://base/";
    protected boolean stop = false;
    protected final RDFStream rdfStream;
    protected final long totalDelay;

    /**
     * @param rdfStream
     * @param totalDelay
     */
    public RSPQLStarStream(RDFStream rdfStream, long totalDelay){
        this.rdfStream = rdfStream;
        this.totalDelay = totalDelay;
    }

    /**
     * Push timestamped graph to RDF stream.
     *
     * @param timestampedGraph
     */
    public void push(TimestampedGraph timestampedGraph){
        rdfStream.push(timestampedGraph);
    }

    /**
     * Convenience method for pushing quads to a stream with a predictable timing. The delay is the total delay of the
     * entire push. If the push is not completed in time an exception is thrown.
     *
     * @param timestampedGraph
     * @param totalDelay
     */
    public void delayedPush(TimestampedGraph timestampedGraph, long totalDelay){
        final long start = System.currentTimeMillis();
        push(timestampedGraph);
        final long end = System.currentTimeMillis();
        final long delay = totalDelay - (end-start);
        busyWaitMillisecond(delay);
    }

    public static void busyWaitMillisecond(long milliseconds){
        long waitUntil = System.nanoTime() + (milliseconds * 1_000_000);
        while(waitUntil > System.nanoTime()){
            ;
        }
    }

    public void stop(){
        stop = true;
    }
}
