package se.liu.ida.rspqlstar.store.engine;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.engine.*;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.dataset.WindowDatasetGraph;

import java.io.PrintStream;
import java.util.Date;

public class RSPQLStarQueryExecution extends QueryExecutionBase {
    protected RSPQLStarQuery query;
    private QueryIterator queryIterator = null;
    public StreamingDatasetGraph sdg;
    private boolean closed;

    public RSPQLStarQueryExecution(RSPQLStarQuery query, StreamingDatasetGraph sdg){
        this(query, DatasetFactory.wrap(sdg));
        this.query = query;
        this.sdg = sdg;
    }

    public RSPQLStarQueryExecution(RSPQLStarQuery query, Dataset dataset) {
        super(query, dataset, ARQ.getContext(), QueryEngineRegistry.get().find(query, dataset.asDatasetGraph()));
    }

    public ResultSet asResultSet(QueryIterator qIter) {
        ResultSetStream rStream = new ResultSetStream(query.getResultVars(), ModelFactory.createDefaultModel(), qIter);
        return rStream;
    }

    public ResultSet execSelect() {
        checkNotClosed();
        if (!query.isSelectType()) {
            throw new QueryExecException("Wrong query type: " + query);
        } else {
            final ResultSet rs = execResultSet();
            return new ResultSetCheckCondition(rs, this);
        }
    }

    private ResultSet execResultSet() {
        startQueryIterator();
        return asResultSet(queryIterator);
    }

    private void checkNotClosed() {
        if (closed) {
            throw new QueryExecException("HTTP QueryExecution has been closed");
        }
    }

    /**
     * Does not support timeout.
     */
    private void startQueryIterator() {
        execInit();
        queryIterator = getPlan().iterator();
    }

    /**
     * Run periodic execution of query.
     * @param out
     */
    public void execContinuousSelect(PrintStream out) {
        while(true) {
            final long t0 = System.currentTimeMillis();
            sdg.setTime(new Date());
            final RSPQLStarQueryExecution exec = new RSPQLStarQueryExecution(query, sdg);
            final ResultSet rs = exec.execSelect();
            out.printf("--- %s ---\n", sdg.getTime());
            ResultSetMgr.write(out, rs, ResultSetLang.SPARQLResultSetText);
            exec.close();

            final long t1 = System.currentTimeMillis();
            out.printf("Query executed in %s ms\n", t1-t0);
            try {
                Thread.sleep(query.getComputedEvery().toMillis());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /*
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            sdg.setTime(new Date());
            out.printf("--- %s ---\n", sdg.getTime());
            final RSPQLStarQueryExecution exec = new RSPQLStarQueryExecution(query, sdg);
            final ResultSet rs = exec.execSelect();
            ResultSetMgr.write(out, rs, ResultSetLang.SPARQLResultSetText);
        }, query.getComputedEvery().toMillis(), query.getComputedEvery().toMillis(), TimeUnit.MILLISECONDS);
        */
    }

}
