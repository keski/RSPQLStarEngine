package se.liu.ida.rspqlstar.store.engine;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QueryEngineMainQuad;
import org.apache.jena.sparql.util.Context;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;

public class RSPQLStarEngine extends QueryEngineMainQuad {
    static final private QueryEngineFactory factory = new QueryEngineFactory() {
        @Override
        public boolean accept(Query query, DatasetGraph datasetGraph, Context context) {
            return datasetGraph instanceof DatasetGraphStar;
        }

        @Override
        public Plan create(Query query, DatasetGraph datasetGraph, Binding binding, Context context) {
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

    public RSPQLStarEngine(Op op, DatasetGraph dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    public RSPQLStarEngine(Query query, DatasetGraph dataset, Binding input, Context context) {
        super(query, dataset, input, context);
    }

    static public void register() {
        QueryEngineRegistry.addFactory(factory);
    }


}
