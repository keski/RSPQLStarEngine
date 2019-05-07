package se.liu.ida.rspqlstar.store.engine;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCheck;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.QueryEngineMainQuad;
import org.apache.jena.sparql.util.Context;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.algebra.MyAlgebra;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;
import se.liu.ida.rspqlstar.store.engine.main.OpRSPQLStarExecutor;

public class RSPQLStarEngine extends QueryEngineMainQuad {
    private static final Logger logger = Logger.getLogger(RSPQLStarEngine.class);
    private Op cachedOp = null;

    static final private QueryEngineFactory factory = new QueryEngineFactory() {
        @Override
        public boolean accept(Query query, DatasetGraph datasetGraph, Context context) {
            return datasetGraph instanceof DatasetGraph;
        }

        @Override
        public Plan create(Query q, DatasetGraph datasetGraph, Binding binding, Context context) {
            RSPQLStarQuery query = (RSPQLStarQuery) q;
            final RSPQLStarEngine engine = new RSPQLStarEngine(query, datasetGraph, binding, context);
            return engine.getPlan();
        }

        @Override
        public boolean accept(Op op, DatasetGraph datasetGraph, Context context) {
            return datasetGraph instanceof DatasetGraphStar;
        }

        @Override
        public Plan create(Op op, DatasetGraph datasetGraph, Binding binding, Context context) {
            final RSPQLStarEngine engine = new RSPQLStarEngine(op, datasetGraph, binding, context);
            return engine.getPlan();
        }
    };

    public RSPQLStarEngine(Op op, DatasetGraph datasetGraph, Binding input, Context context) {
        super(op, datasetGraph, input, context);
        QC.setFactory(context, OpRSPQLStarExecutor.factory);
    }

    public RSPQLStarEngine(Query query, DatasetGraph datasetGraph, Binding input, Context context) {
        super(query, datasetGraph, input, context);
        QC.setFactory(context, OpRSPQLStarExecutor.factory);
    }

    static public void register() {
        QueryEngineRegistry.addFactory(factory);
    }

    protected Op createOp(Query query) {
        if(cachedOp == null) {
            cachedOp = MyAlgebra.compile(query);
        }
        logger.debug(cachedOp);
        return cachedOp;
    }

    public QueryIterator eval(Op op, DatasetGraph datasetGraph, Binding input, Context context) {
        final ExecutionContext execCxt = new ExecutionContext(context, null, datasetGraph, QC.getFactory(context));
        final QueryIterator iter1 = QueryIterRoot.create(input, execCxt);
        final QueryIterator iter2 = QC.execute(op, iter1, execCxt);
        return QueryIteratorCheck.check(iter2, execCxt); // check for closed iterators
    }

    @Override
    protected Op modifyOp(Op op) {
        return op;
    }
}
