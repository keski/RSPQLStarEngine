package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import se.liu.ida.rspqlstar.algebra.op.OpWindow;
import se.liu.ida.rspqlstar.algebra.op.OpWrapper;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;

import java.util.*;

public class RSPQLStarTransform extends TransformCopy {
    final private HashMap<Node_Triple, Var> varMap = new HashMap();
    final private VarDictionary varDict = VarDictionary.get();

    @Override
    public Op transform(final OpExt opExt) {
        if(opExt instanceof OpWindow){
            final OpWindow opWindow = (OpWindow) opExt;
            return xform(opWindow, opWindow.getSubOp());
        } else {
            return super.transform(opExt);
        }
    }

    private OpWindow xform(final OpWindow op, final Op subOp) {
        return op.getSubOp() == subOp ? op : op.copy(subOp);
    }

    @Override
    public Op transform(final OpQuadPattern opQuadPattern) {
        return createJoinTree(opQuadPattern);
    }

    @Override
    public Op transform(final OpSequence opSequence, final List<Op> elts) {
        return createJoinTree(opSequence);
    }

    /*
    @Override
    public Op transform(final OpProject opProject, final Op subOp) {
        System.err.println("Found OpProject");
        System.err.println(subOp);
        //if(true) throw new IllegalStateException();
        //final Op subOpJoinTree = createJoinTree(subOp);
        return new OpProject(subOp, opProject.getVars());
    }
    */

    /////////////////////////////////////////////////////////////////////////////
    // Re-used to a large extent

    /**
     * Create join tree. By changing the order of the query operators the efficiency of the query
     * can be increased.
     * @param op
     * @return
     */
    private Op createJoinTree(final Op op) {
        System.err.println("Op is: " + op.getClass().getName());
        System.err.println(op);
        System.err.println();

        final ArrayList<OpWrapper> opWrappers = new ArrayList<>();
        final List<OpWrapper> unusedOpWrappers = opToList(op);

        // Iterate all ops, create joins and add to opWrappers
        while (unusedOpWrappers.size() > 0) {
            OpWrapper headOfTree = getOpWithHighestSelectivity(unusedOpWrappers, new ArrayList<>());
            unusedOpWrappers.remove(headOfTree);

            // Create joins for the current headOfTree
            while (containsJoin(headOfTree, unusedOpWrappers)) {
                OpWrapper child = getOpWithHighestSelectivityThatJoins(headOfTree, unusedOpWrappers);
                unusedOpWrappers.remove(child);
                headOfTree = opJoin(headOfTree, child);
            }
            opWrappers.add(headOfTree);
        }

        // if no more unused opWrappers
        if(unusedOpWrappers.size() == 0){
            System.err.println(op);
            System.err.println("only one");
            return opWrappers.get(0).asOp();
        }

        // If still some unused opWrappers
       // Iterate all opWrappers, create sequence
        OpWrapper headOfTree = null;
        for (OpWrapper opWrapper : opWrappers) {
            if (headOfTree != null) {
                headOfTree = new OpWrapper(OpSequence.create(headOfTree.asOp(), opWrapper.asOp()));
            } else {
                headOfTree = opWrapper;
            }
        }
        return headOfTree.asOp();

    }

    /**
     * Create a list of OpWrappers from an Op. Each op is split and wrapped.
     *
     * TODO extend for other part or add default?
     */
    private List<OpWrapper> opToList(Op op) {
        final List<OpWrapper> wrappedOps = new ArrayList<>();

        if (op instanceof OpQuadPattern) {
            for (Op op2 : splitOp((OpQuadPattern) op)) {
                wrappedOps.add(new OpWrapper(op2));
            }
        } else if (op instanceof OpQuad) {
            for (Op op2 : splitOp((OpQuad) op)) {
                wrappedOps.add(new OpWrapper(op2));
            }
        } else if (op instanceof OpSequence) {
            for (Op op2 : splitOp((OpSequence) op)) {
                wrappedOps.addAll(opToList(op2));
            }
        } else if (op instanceof OpExtend) {
            for (Op op2 : splitOp((OpExtend) op)) {
                wrappedOps.add(new OpWrapper(op2));
            }
        } else if (op instanceof OpFilter) {
            wrappedOps.add(new OpWrapper(op));
        } else {
            System.err.println("Failed to split op: " + op);
            wrappedOps.add(new OpWrapper(op));
            //throw new IllegalStateException();
        }

        return wrappedOps;
    }

    private List<Op> splitOp(OpSequence opSequence) {
        final List<Op> result = new ArrayList<>();
        for (Op op : opSequence.getElements()) {
            if (op instanceof OpQuadPattern) {
                result.addAll(splitOp((OpQuadPattern) op));
            } else if (op instanceof OpQuad) {
                result.addAll(splitOp((OpQuad) op));
            } else if (op instanceof OpSequence) {
                result.addAll(splitOp((OpSequence) op));
            } else if (op instanceof OpExtend) {
                result.addAll(splitOp((OpExtend) op));
            } else if(op instanceof OpFilter){
                result.add(op);
            } else if(op instanceof OpTable){
                result.add(op);
            } else {
                System.err.println("Failed to split op: " + op);
                throw new IllegalStateException();
            }
        }
        return result;
    }

    private List<Op> splitOp(OpQuadPattern opQuadPattern) {
        final List<Op> result = new ArrayList<>();
        for (Quad quad : opQuadPattern.getPattern()) {
            if (containsEmbeddedTriple(quad)) {
                final List<Op> ops = splitQuadWithEmbeddedTriple(quad);
                result.addAll(ops);
            } else {
                result.add(new OpQuad(quad));
            }
        }
        return result;
    }

    private List<Op> splitOp(OpQuad opQuad) {
        final List<Op> result = new ArrayList<>();
        final Quad quad = opQuad.getQuad();
        if (containsEmbeddedTriple(quad)) {
            result.addAll(splitQuadWithEmbeddedTriple(quad));
        } else {
            result.add(new OpQuad(quad));
        }
        return result;
    }

    private List<Op> splitOp(OpExtend opExtend) {
        final List<Op> result = new ArrayList<>();
        final OpExtend extend = opExtend;

        for (Map.Entry<Var, Expr> element : extend.getVarExprList().getExprs().entrySet()) {
            result.add(OpExtend.create(OpTable.empty(), element.getKey(), element.getValue()));
        }
        return result;
    }

    private boolean containsEmbeddedTriple(Quad quad) {
        return quad.getSubject() instanceof Node_Triple || quad.getObject() instanceof Node_Triple;
    }

    private List<Op> splitQuadWithEmbeddedTriple(Quad quad) {
        final List<Op> split = new ArrayList<>();

        final Node subject;
        final Node predicate = quad.getPredicate();
        final Node object;

        if (quad.getSubject() instanceof Node_Triple) {
            final Node_Triple s = (Node_Triple) quad.getSubject();
            final NodeValueNode exp = new NodeValueNode(s);
            final Var var;

            if (varMap.containsKey(s)) {
                var = varMap.get(s);
            } else {
                var = varDict.getFreshVariable();
                varMap.put(s, var);
                split.add(OpExtend.create(OpTable.empty(), var, exp));
            }
            subject = var;
        } else {
            subject = quad.getSubject();
        }

        if (quad.getObject() instanceof Node_Triple) {
            final Node_Triple o = (Node_Triple) quad.getObject();
            final NodeValueNode exp = new NodeValueNode(o);
            final Var var;

            if (varMap.containsKey(o)) {
                var = varMap.get(o);
            } else {
                var = varDict.getFreshVariable();
                varMap.put(o, var);
                split.add(OpExtend.create(OpTable.empty(), var, exp));
            }

            object = var;
        } else {
            object = quad.getObject();
        }

        split.add(new OpQuad(new Quad(quad.getGraph(), subject, predicate, object)));
        return split;
    }


    /**
     * Remove and return the op with the highest selectivity that joins with the operator tree (or null
     * of no op can be joined with the operator tree).
     *
     * @param operatorTree
     * @param opWrappers
     * @return The opwrapper to be added to the operator tree.
     */
    private OpWrapper getOpWithHighestSelectivityThatJoins(OpWrapper operatorTree, List<OpWrapper> opWrappers) {
        final List<Var> vars = new ArrayList<>();
        vars.addAll(operatorTree.getVariables());
        OpWrapper highest = null;

        for (OpWrapper opWrapper : opWrappers) {
            if (!Collections.disjoint(vars, opWrapper.getVariables())) {

                if (highest == null) {
                    highest = opWrapper;
                }

                if (highest.calculateSelectivity(vars) < opWrapper.calculateSelectivity(vars)) {
                    highest = opWrapper;
                }
            }
        }
        opWrappers.remove(highest);
        return highest;
    }

    /**
     * Traverse the list of OpWappers and find the element with the highest selectivity, see {@link SelectivityMap}
     * That element is removed from the list and returned.
     */
    private OpWrapper getOpWithHighestSelectivity(List<OpWrapper> opWrappers, List<Var> vars) {
        OpWrapper highest = null;
        for (OpWrapper opWrapper : opWrappers) {
            if (highest == null) {
                highest = opWrapper;
            } else if (highest.calculateSelectivity(vars) < opWrapper.calculateSelectivity(vars)) {
                highest = opWrapper;
            }
        }
        opWrappers.remove(highest);
        return highest;
    }

    /**
     * Return true iff the op joins with a least one other op.
     */
    private boolean containsJoin(OpWrapper op, List<OpWrapper> opWrappers) {
        final List<Var> variables = new ArrayList<>();
        variables.addAll(op.getVariables());
        for (OpWrapper opWrapper : opWrappers) {
            if (!Collections.disjoint(variables, opWrapper.getVariables())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Create a join between the opWrappers
     * @param left
     * @param right
     * @return
     */
    private OpWrapper opJoin(OpWrapper left, OpWrapper right) {
        return new OpWrapper(OpJoin.create(left.asOp(), right.asOp()));
    }
}
