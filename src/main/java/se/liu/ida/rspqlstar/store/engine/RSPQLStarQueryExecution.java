package se.liu.ida.rspqlstar.store.engine;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.engine.*;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.PrintStream;

public class RSPQLStarQueryExecution extends QueryExecutionBase {
    protected RSPQLStarQuery query;
    private QueryIterator queryIterator = null;
    public StreamingDatasetGraph sdg;
    private boolean closed;
    private boolean stop = false;

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
        stop = false;
        while(!stop) {
            final long t0 = System.currentTimeMillis();
            sdg.setTime(TimeUtil.getTime());
            final RSPQLStarQueryExecution exec = new RSPQLStarQueryExecution(query, sdg);
            final ResultSet rs = exec.execSelect();
            out.printf("\n### %s ###\n", TimeUtil.df.format(sdg.getTime()));
            if(!rs.hasNext()) out.println("--- Empty result ---");
            else {
                ResultSetMgr.write(out, rs, ResultSetLang.SPARQLResultSetText);
            }
            exec.close();

            final long execTime = System.currentTimeMillis() - t0;
            out.printf("Query executed in %s ms\n", execTime);

            busyWaitMillisecond(query.getComputedEvery().toMillis() - execTime);
        }
    }

    public static void busyWaitMillisecond(long milliseconds){
        long waitUntil = System.nanoTime() + (milliseconds * 1_000_000);
        while(waitUntil > System.nanoTime()){ }
    }

    public void stopContinuousSelect(){
        stop = true;
    }

}
