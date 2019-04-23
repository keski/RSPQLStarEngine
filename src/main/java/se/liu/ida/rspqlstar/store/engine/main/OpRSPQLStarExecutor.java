package se.liu.ida.rspqlstar.store.engine.main;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.atlas.iterator.Iter;
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
import org.apache.jena.sparql.expr.NodeValue;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.main.iterator.*;
import se.liu.ida.rspqlstar.store.engine.main.quadpattern.*;

import java.util.Iterator;

public class OpRSPQLStarExecutor extends OpExecutor {

    static final public OpExecutorFactory factory = execCxt -> new OpRSPQLStarExecutor(execCxt);
    final private VarDictionary varDict = VarDictionary.get();
    final private NodeDictionary nd = NodeDictionaryFactory.get();
    public Key activeGraph;

    /**
     * Creates an operator compiler.
     */
    public OpRSPQLStarExecutor(ExecutionContext execCxt) {
        super(execCxt);
    }

    /**
     * Map to ID based OpJoin.
     * @param opJoin
     * @param input
     * @return
     */
    @Override
    protected QueryIterator execute(OpJoin opJoin, QueryIterator input) {
        // Encode incoming iterator into ID based bindings
        Iterator<SolutionMapping> iter = new EncodeBindingsIterator(input, execCxt);
        // Return a decoded standard iterator
        return new DecodeBindingsIterator(execute(opJoin, iter), execCxt);
    }

    /**
     * Map to ID based OpSequence.
     * @param opSequence
     * @param input
     * @return
     */
    @Override
    protected QueryIterator execute(OpSequence opSequence, QueryIterator input) {
        // Encode incoming iterator into ID based bindings
        Iterator<SolutionMapping> iter = new EncodeBindingsIterator(input, execCxt);
        // Return a decoded standard iterator
        return new DecodeBindingsIterator(execute(opSequence, iter), execCxt);
    }

    /**
     * Map to ID based OpExtend.
     * @param opExtend
     * @param input
     * @return
     */
    @Override
    protected QueryIterator execute(OpExtend opExtend, QueryIterator input) {
        // Encode incoming iterator into ID based bindings
        final Iterator<SolutionMapping> iter = new EncodeBindingsIterator(input, execCxt);
        // Return a decoded standard iterator
        return new DecodeBindingsIterator(execute(opExtend, iter), execCxt);
    }

    //////////////////////////////////////////////////////////////////////////////////

    /**
     * ID based OpJoin.
     *
     * @param opJoin
     * @param input
     * @return
     */
    protected Iterator<SolutionMapping> execute(final OpJoin opJoin, final Iterator<SolutionMapping> input){
        final Op opLeft = opJoin.getLeft();
        final Op opRight = opJoin.getRight();
        final Iterator<SolutionMapping> iterator = executeIdBasedOp(opLeft, input);
        return executeIdBasedOp(opRight, iterator);
    }
    /**
     * ID based OpQuad.
     *
     * @param opQuad
     * @param input
     * @return
     */
    protected Iterator<SolutionMapping> execute(final OpQuad opQuad, final Iterator<SolutionMapping> input){
        return new IdBasedQuadPatternIterator(encode(opQuad), input, execCxt);
    }

    /**
     * ID based OpSequence.
     *
     * @param opSequence
     * @param input
     * @return
     */
    private Iterator<SolutionMapping> execute(final OpSequence opSequence, final Iterator<SolutionMapping> input) {
        Iterator<SolutionMapping> iterator = input;
        for (Op op : opSequence.getElements()) {
            iterator = executeIdBasedOp(op, iterator);
        }
        return iterator;
    }

    protected Iterator<SolutionMapping> execute(final OpTable opTable, Iterator<SolutionMapping> input) {
        if (opTable.isJoinIdentity()) {
            return input;
        }
        throw new IllegalStateException();
    }

    /**
     * ID based OpFilter.
     *
     * @param opFilter
     * @param input
     * @return
     */
    protected Iterator<SolutionMapping> execute(OpFilter opFilter, Iterator<SolutionMapping> input){
        //return input;
        final QueryIterator iter = new DecodeBindingsIterator(input, execCxt);
        return new EncodeBindingsIterator(execute(opFilter, iter), execCxt);
    }


    private Iterator<SolutionMapping> execute(OpExtend opExtend, Iterator<SolutionMapping> input) {
        final Expr expr = opExtend.getVarExprList().getExprs().values().iterator().next();
        final Node node = ((NodeValue) expr).asNode();
        final Var var = opExtend.getVarExprList().getVars().get(0);

        if (node instanceof Node_Triple) {
            // Embedded triple extend
            final Triple t = ((Node_Triple) node).get();
            final Element s = encodeAsElement(t.getSubject());
            final Element p = encodeAsElement(t.getPredicate());
            final Element o = encodeAsElement(t.getObject());
            final QuadStarPattern pattern = new QuadStarPattern(activeGraph, s, p, o);
            return new IdBasedExtendWithEmbeddedTriplePattern(encode(var), pattern, input, execCxt);
        } else {
            // regular extend
            final Long id = encode(node);
            final Key key = id != null ? new Key(id) : new NodeWrapperKey(node);
            return new IdBasedExtendIterator(encode(var), key, input, execCxt);
        }

        //throw new IllegalStateException("The opExtend does not seem to contain any expression: " + opExtend.getVarExprList());
    }

    public Iterator<SolutionMapping> executeIdBasedOp(final Op op, final Iterator<SolutionMapping> input){
        final Iterator<SolutionMapping> iterator;
        if (op instanceof OpQuad) {
            iterator = execute((OpQuad) op, input);
        } else if (op instanceof OpJoin) {
            iterator = execute((OpJoin) op, input);
        } else if (op instanceof OpSequence) {
            iterator = execute((OpSequence) op, input);
        } else if (op instanceof OpExtend) {
            iterator = execute((OpExtend) op, input);
        } else if (op instanceof OpFilter) {
            iterator = execute((OpFilter) op, input);
        } else if(op instanceof OpTable) {
            iterator = execute((OpTable) op, input);
        } else {
            System.err.println("There is no id-based iterator implemented for " + op);
            System.err.println("Defaulting to decode/encode");
            QueryIterator iter = exec(op, new DecodeBindingsIterator(input, execCxt));
            return new EncodeBindingsIterator(iter, execCxt);
        }
        return iterator;
    }


    /**
     * Encode quad as QuadStarPattern
     * @param opQuad
     * @return
     */
    private QuadStarPattern encode(OpQuad opQuad) {
        final Quad pattern = opQuad.getQuad();
        final QuadPatternBuilder builder = new QuadPatternBuilder();
        builder.setGraph(pattern.getGraph());
        builder.setSubject(pattern.getSubject());
        builder.setPredicate(pattern.getPredicate());
        builder.setObject(pattern.getObject());
        return builder.createQuadPattern();
    }

    /**
     * Encode Node_Triple as QuadStarPattern.
     *
     * @param node_triple
     * @return
     */
    private QuadStarPattern encode(Node graph, Node_Triple node_triple) {
        final QuadPatternBuilder builder = new QuadPatternBuilder();
        final Triple t = node_triple.get();
        builder.setGraph(graph);
        builder.setSubject(t.getSubject());
        builder.setPredicate(t.getPredicate());
        builder.setObject(t.getObject());

        return builder.createQuadPattern();
    }

    /**
     * Encode Node or Variable as key.
     *
     * @param node
     * @return
     */
    private Long encode(Node node) {
        return nd.getId(node);
    }

    /**
     * Encode Var as integer.
     *
     * @param var
     * @return
     */
    private int encode(Var var) {
        return varDict.createId(var);
    }

    private Element encodeAsElement(Node node){
        final Element el;
        if(node.isVariable()){
            final int varId = encode((Var) node);
            el = new Variable(varId);
        } else {
            final Long id = encode(node);
            if(id == null) {
                el = new NodeWrapperKey(node);
            } else {
                el = new Key(id);
            }
        }
        return el;
    }
}
