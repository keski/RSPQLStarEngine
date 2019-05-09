package se.liu.ida.rspqlstar.store.engine;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.engine.*;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.PrintStream;
import java.util.ArrayList;

public class RSPQLStarQueryExecution extends QueryExecutionBase {
    private final Logger logger = Logger.getLogger(RSPQLStarQuery.class);
    protected RSPQLStarQuery query;
    private QueryIterator queryIterator = null;
    public StreamingDatasetGraph sdg;
    private boolean closed;
    private boolean stop = false;

    // Collection of experiment results
    public ArrayList<Long> expResults = new ArrayList<>();

    public RSPQLStarQueryExecution(RSPQLStarQuery query, StreamingDatasetGraph sdg){
        this(query, DatasetFactory.wrap(sdg));
        this.query = query;
        this.sdg = sdg;
        if(!sdg.isReady()){
            sdg.initForQuery(query);
        }

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
            final long t0 = System.nanoTime();
            sdg.setTime(TimeUtil.getTime());
            final RSPQLStarQueryExecution exec = new RSPQLStarQueryExecution(query, sdg);
            final ResultSet rs = exec.execSelect();

            out.printf("Application time: %s\n", TimeUtil.df.format(sdg.getTime()));
            if(!rs.hasNext()){
                out.println("| Empty result |");
            }
            else {
                ResultSetMgr.write(out, rs, ResultSetLang.SPARQLResultSetText);
            }
            exec.close();

            final long execTime = System.nanoTime() - t0;
            out.printf("Query executed in %s ms\n\n", execTime/1_000_000);

            // save
            expResults.add(execTime);

            long delay = query.getComputedEvery().toMillis() - execTime/(1_000_000);
            if(delay > 0) TimeUtil.silentSleep(delay);
            //busyWaitMillisecond(query.getComputedEvery().toMillis() - execTime/(1_000_000));
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
