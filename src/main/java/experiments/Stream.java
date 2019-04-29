package experiments;

import se.liu.ida.rspqlstar.store.dataset.RDFStream;
import se.liu.ida.rspqlstar.store.dataset.TimestampedGraph;

abstract class Stream implements Runnable {
    protected final RDFStream rdfStream;
    protected final long totalDelay;

    /**
     * @param rdfStream
     * @param totalDelay
     */
    public Stream(RDFStream rdfStream, long totalDelay){
        this.rdfStream = rdfStream;
        this.totalDelay = totalDelay;
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
        rdfStream.push(timestampedGraph);
        final long end = System.currentTimeMillis();
        long delay = totalDelay - (end-start);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }
}
