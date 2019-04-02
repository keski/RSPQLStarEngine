package se.liu.ida.rspqlstar.store.queryengine;


import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpModifier;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCheck;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.util.Context;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;
import se.liu.ida.rspqlstar.store.transform.MyTransform;

/**
 * The query engine that replaces the standard Jena query engine. The OpStarExecutor {@link OpStarExecutor} is
 * registered and the transformer {@link MyTransform} is added during during evaluation.
 */
public class QueryEngineStar extends QueryEngineMain {
    static private VarDictionary varDict = VarDictionary.get();

    /**
     * The factory object that creates a queryEngine.
     */
    static final private QueryEngineFactory factory = new QueryEngineFactory() {
        public boolean accept(Query query, DatasetGraph ds, Context cxt) {
            return isIdBased(ds);
        }

        public boolean accept(Op op, DatasetGraph ds, Context cxt) {
            return isIdBased(ds);
        }

        public Plan create(Query query, DatasetGraph dataset, Binding initialBinding, Context context) {
            final QueryEngineStar engine = new QueryEngineStar(query, dataset, initialBinding, context);
            return engine.getPlan();
        }

        public Plan create(Op op, DatasetGraph dataset, Binding initialBinding, Context context) {
            final QueryEngineStar engine = new QueryEngineStar(op, dataset, initialBinding, context);
            return engine.getPlan();
        }

        private boolean isIdBased(DatasetGraph ds) {
            return (ds.getDefaultGraph() instanceof DatasetGraphStar);
        }
    };

    /**
     * Registers this engine so that it can be selected for query execution.
     */
    static public void register() {
        QueryEngineRegistry.addFactory(factory);
    }

    // initialization methods

    public QueryEngineStar(Op op, DatasetGraph dataset, Binding input, Context context) {
        super(op, dataset, input, context);
        registerOpExecutor();
    }

    public QueryEngineStar(Query query, DatasetGraph dataset, Binding input, Context context) {
        super(query, dataset, input, context);
        registerOpExecutor();
    }

    private void registerOpExecutor() {
        QC.setFactory(context, OpStarExecutor.factory);
    }

    // operations
    @Override
    public QueryIterator eval(Op op, DatasetGraph dsg, Binding input, Context context) {
        final Op opTransform = Transformer.transform(new MyTransform(), op);
        ExecutionContext execCxt = createExecutionContext(opTransform, dsg, context);
        return createIteratorChain(opTransform, input, execCxt);
    }

    // helpers
    protected ExecutionContext createExecutionContext(Op op, DatasetGraph dsg, Context contextP) {
        initializeVarDictionary(op);
        return new ExecutionContext(contextP,
                dsg.getDefaultGraph(),
                dsg,
                QC.getFactory(contextP));
    }

    protected QueryIterator createIteratorChain(Op op, Binding input, ExecutionContext execCxt) {
        final QueryIterator qIter1 = QueryIterRoot.create(input, execCxt);
        final QueryIterator qIter2 = QC.execute(op, qIter1, execCxt);
        return QueryIteratorCheck.check(qIter2, execCxt); // check for closed iterators
    }

    /**
     * Creates a dictionary of query variables that knows all variables in the
     * operator tree of which the given operator is the root.
     */
    final protected void initializeVarDictionary(Op op) {
        // We cannot call OpVars.allVars(op) directly because it does not
        // consider all variables in sub-operators of OpProject. Hence,
        // we simply strip the solution modifiers and, thus, call the
        // method for the first operator that is not a solution modifier.
        varDict.reset();
        Op tmp = op;
        while (tmp instanceof OpModifier) {
            tmp = ((OpModifier) tmp).getSubOp();
        }

        for (Var v : OpVars.visibleVars(tmp)) {
            varDict.createId(v);
        }
    }
}
