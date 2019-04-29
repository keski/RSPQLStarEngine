package se.liu.ida.rspqlstar.store.engine;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCheck;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.QueryEngineMainQuad;
import org.apache.jena.sparql.util.Context;
import se.liu.ida.rspqlstar.algebra.MyAlgebra;
import se.liu.ida.rspqlstar.algebra.RSPQLStarTransformSimple;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;
import se.liu.ida.rspqlstar.store.engine.main.OpRSPQLStarExecutor;

public class RSPQLStarEngine extends QueryEngineMainQuad {
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
        return MyAlgebra.compile(query);
    }

    public QueryIterator eval(Op op, DatasetGraph datasetGraph, Binding input, Context context) {
        final ExecutionContext execCxt = new ExecutionContext(context, null, datasetGraph, QC.getFactory(context));
        final QueryIterator qIter1 = QueryIterRoot.create(input, execCxt);
        final QueryIterator qIter2 = QC.execute(op, qIter1, execCxt);
        return QueryIteratorCheck.check(qIter2, execCxt); // check for closed iterators
    }


}
