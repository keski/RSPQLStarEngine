package se.liu.ida.rspqlstar.stream;

import se.liu.ida.rspqlstar.store.dataset.RDFStream;
import se.liu.ida.rspqlstar.store.dataset.TimestampedGraph;
import se.liu.ida.rspqlstar.util.TimeUtil;

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
     * @param delay
     */
    public void delayedPush(TimestampedGraph timestampedGraph, long delay){
        push(timestampedGraph);
        TimeUtil.silentSleep(delay);
        //busyWaitMillisecond(delay);
    }

    /**
     * Busy waiting is much more accurate, but very resource expensive. As a trade-off,
     * this method uses Thread.sleep for the majority of the duration and then aligns it
     * by using busy wait.

     * @param milliseconds
     */
    public static void busyWaitMillisecond(long milliseconds){
        final long t0 = System.currentTimeMillis();
        // Use sleep for most of the duration
        if(milliseconds > 100){
            TimeUtil.silentSleep(milliseconds-50);
        }
        final long t1 = System.currentTimeMillis();
        final long waitUntil = t1 + milliseconds - (t1-t0);
        while(waitUntil > System.currentTimeMillis()){
            ;
        }
    }

    public void stop(){
        stop = true;
    }
}
