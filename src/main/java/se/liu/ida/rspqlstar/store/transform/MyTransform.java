package se.liu.ida.rspqlstar.store.transform;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.utils.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * The class contains the logic for the query optimization.
 */
public class MyTransform extends TransformCopy {
    final private HashMap<Node_Triple, Var> varMap = new HashMap();
    final private VarDictionary varDict = VarDictionary.get();

    @Override
    public Op transform(OpBGP opBGP) {
        return createJoinTree(opBGP);
    }

    @Override
    public Op transform(OpSequence opSequence, List<Op> elts) {
        return createJoinTree(opSequence);
    }

    private List<Op> splitTripleWithEmbeddedTriple(Triple triple) {
        final List<Op> split = new ArrayList<>();

        final Node subject;
        if (triple.getSubject() instanceof Node_Triple) {
            final Node_Triple s = (Node_Triple) triple.getSubject();
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
            subject = triple.getSubject();
        }
        final Node predicate = triple.getPredicate();
        final Node object;
        if (triple.getObject() instanceof Node_Triple) {
            final Node_Triple o = (Node_Triple) triple.getObject();
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
            object = triple.getObject();
        }

        final Op op = new OpTriple(new Triple(subject, predicate, object));

        if(Configuration.reverseBindSplitting){
            split.add(0, op);
        } else {
            split.add(op);
        }
        return split;
    }

    private boolean containsEmbeddedTriple(Triple triple) {
        if (triple.getSubject() instanceof Node_Triple) {
            return true;
        } else if (triple.getObject() instanceof Node_Triple) {
            return true;
        } else {
            return false;
        }
    }

    private Op createJoinTree(Op op) {
        final ArrayList<OpWrapper> opWrappers = new ArrayList<>();
        final List<OpWrapper> unusedOpWrappers = opToList(op);

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
     * Return true iff there exist at least two OPs that share a variable.
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
     *
     * @param left  The old treeHead should be the left.
     * @param right The new opWrapper should be right.
     * @return A OpJoin that is head of the tree
     */
    private OpWrapper opJoin(OpWrapper left, OpWrapper right) {
        return new OpWrapper(OpJoin.create(left.asOp(), right.asOp()));
    }

    /**
     * The op with the highest selectivity (that joins) is removed from the list and returned.
     *
     * @param operatorTree, used to find join candidates
     * @param opWrappers,   list of unused opwrappers.
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
     * Create a list of OpWrappers
     */
    private List<OpWrapper> opToList(Op op) {
        final List<OpWrapper> wrappedOp = new ArrayList<>();
        if (op instanceof OpBGP) {
            for (Op element : splitBGP(op)) {
                wrappedOp.add(new OpWrapper(element));
            }
        } else if (op instanceof OpSequence) {
            for (Op element : splitSequence(op)) {
                wrappedOp.add(new OpWrapper(element));
            }
        } else if (op instanceof OpExtend) {
            for (Op element : splitExtend(op)) {
                wrappedOp.add(new OpWrapper(element));
            }
        } else {
            throw new NotImplementedException("no support of " + op.getName());
        }

        return wrappedOp;
    }

    private List<Op> splitSequence(Op op) {
        final List<Op> result = new ArrayList<>();
        final OpSequence sequence = (OpSequence) op;
        for (Op element : sequence.getElements()) {
            if (element instanceof OpBGP) {
                result.addAll(splitBGP(element));
            } else if (element instanceof OpExtend) {
                result.addAll(splitExtend(element));
            } else {
                throw new NotImplementedException("no support for " + element.getName());
            }
        }
        return result;
    }

    private List<Op> splitExtend(Op op) {
        final List<Op> result = new ArrayList<>();
        final OpExtend extend = (OpExtend) op;

        for (Entry<Var, Expr> element : extend.getVarExprList().getExprs().entrySet()) {
            result.add(OpExtend.create(OpTable.empty(), element.getKey(), element.getValue()));
        }
        return result;
    }

    private List<Op> splitBGP(Op op) {
        // Variables are bound to a specific bgp
        varMap.clear();
        final List<Op> result = new ArrayList<>();
        final OpBGP bgp = (OpBGP) op;
        for (Triple triple : bgp.getPattern()) {
            if (containsEmbeddedTriple(triple)) {
                result.addAll(splitTripleWithEmbeddedTriple(triple));
            } else {
                result.add(new OpTriple(triple));
            }
        }
        return result;
    }
}
