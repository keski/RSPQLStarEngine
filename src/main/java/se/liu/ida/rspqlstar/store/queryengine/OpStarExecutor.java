package se.liu.ida.rspqlstar.store.queryengine;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.triplepattern.QuadPatternBuilder;
import se.liu.ida.rspqlstar.store.triplepattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.triplestore.QuadStore;

import java.util.Iterator;


public class OpStarExecutor extends OpExecutor {

    static final public OpExecutorFactory factory = execCxt -> new OpStarExecutor(execCxt);
    private VarDictionary varDict = VarDictionary.get();
    private Node activeGraph;

    /**
     * Creates an operator compiler.
     */
    public OpStarExecutor(ExecutionContext execCxt) {
        super(execCxt);
        activeGraph = QuadStore.DEFAULT_GRAPH_NODE;
    }

    @Override
    protected QueryIterator execute(OpSequence opSequence, QueryIterator input) {
        return new DecodeBindingsIterator(execute(opSequence, new EncodeBindingsIterator(input, execCxt)), execCxt);
    }

    @Override
    protected QueryIterator execute(OpExtend opExtend, QueryIterator input) {
        if (opExtend.getVarExprList().getExprs().values().iterator().next() instanceof NodeValueNode) {
            return new DecodeBindingsIterator(execute(opExtend, new EncodeBindingsIterator(input, execCxt)), execCxt);
        } else {
            return super.execute(opExtend, input);
        }
    }

    @Override
    public QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        throw new IllegalArgumentException("This iterator should never be called! All BGPs should " +
                "have been rewritten. opBGP: " + opBGP.toString());
    }

    @Override
    public QueryIterator execute(OpTriple opTriple, QueryIterator input) {
        return new DecodeBindingsIterator(execute(opTriple, new EncodeBindingsIterator(input, execCxt)), execCxt);
    }

    @Override
    protected QueryIterator execute(OpJoin opJoin, QueryIterator input) {
        return new DecodeBindingsIterator(execute(opJoin, new EncodeBindingsIterator(input, execCxt)), execCxt);
    }

    private Iterator<SolutionMapping> execute(OpJoin opJoin, Iterator<SolutionMapping> solutionMappingIter) {
        final Op opLeft = opJoin.getLeft();
        final Op opRight = opJoin.getRight();
        final Iterator<SolutionMapping> leftIter;

        if (opLeft instanceof OpTriple) {
            leftIter = execute((OpTriple) opLeft, solutionMappingIter);
        } else if (opLeft instanceof OpJoin) {
            leftIter = execute((OpJoin) opLeft, solutionMappingIter);
        } else if (opLeft instanceof OpExtend) {
            leftIter = execute((OpExtend) opLeft, solutionMappingIter);
        } else {
            throw new NotImplementedException("There is no id-based iterator implemented for " + opLeft);
        }

        if (opRight instanceof OpTriple) {
            return execute((OpTriple) opRight, leftIter);
        } else if (opRight instanceof OpJoin) {
            return execute((OpJoin) opRight, leftIter);
        } else if (opRight instanceof OpExtend) {
            return execute((OpExtend) opRight, leftIter);
        } else {
            throw new NotImplementedException("There is no id-based iterator implemented for " + opRight);
        }
    }

    private Iterator<SolutionMapping> execute(OpSequence opSequence, Iterator<SolutionMapping> solutionMappingIter) {
        Iterator<SolutionMapping> iterator = solutionMappingIter;
        for (Op element : opSequence.getElements()) {
            if (element instanceof OpTriple) {
                iterator = execute((OpTriple) element, iterator);
            } else if (element instanceof OpJoin) {
                iterator = execute((OpJoin) element, iterator);
            } else if (element instanceof OpExtend) {
                iterator = execute((OpExtend) element, iterator);
            } else {
                throw new NotImplementedException("There is no id-based iterator implemented for " + element);
            }
        }
        return iterator;
    }

    private Iterator<SolutionMapping> execute(OpExtend opExtend, Iterator<SolutionMapping> solutionMappingIter) {
        // This for loop can only run once.
        for (Expr expr : opExtend.getVarExprList().getExprs().values()) {
            if (expr instanceof NodeValueNode) {
                final Node_Triple node = (Node_Triple) ((NodeValueNode) expr).asNode();
                final Var var = opExtend.getVarExprList().getVars().get(0);
                // Note that children of OpExtend are not expected and are not handled
                return new ExtendWithEmbeddedTriplePatternQueryIter(encode(var), encode(node), solutionMappingIter, execCxt);
            } else {
                // TODO Handle normal bind. This should use the standard OpExtend from Jena
                throw new NotImplementedException("Only embedded triple patterns are currently supported. OpStarExecutor.execute()");
            }
        }

        throw new IllegalStateException("The opExtend does not seem to contain any expression: " + opExtend.getVarExprList());
    }

    private Iterator<SolutionMapping> execute(OpTriple opTriple, Iterator<SolutionMapping> solutionMappingIter) {
        // TODO add active graph
        return new QuadPatternQueryIter(encode(new Quad(null, opTriple.getTriple())), solutionMappingIter, execCxt);
    }

    /**
     * The index is based on quad patterns and requires the active graph.
     * @param tp
     * @return
     */
    private QuadStarPattern encode(Quad tp) {
        final QuadPatternBuilder builder = new QuadPatternBuilder();
        builder.setGraph(activeGraph);
        builder.setSubject(tp.getSubject());
        builder.setPredicate(tp.getPredicate());
        builder.setObject(tp.getObject());
        return builder.createQuadPattern();
    }

    private QuadStarPattern encode(Node_Triple node_triple) {
        final QuadPatternBuilder builder = new QuadPatternBuilder();
        final Triple t = node_triple.get();
        builder.setGraph(activeGraph);
        builder.setSubject(t.getSubject());
        builder.setPredicate(t.getPredicate());
        builder.setObject(t.getObject());

        return builder.createQuadPattern();
    }

    private int encode(Var var) {
        return varDict.createId(var);
    }
}
