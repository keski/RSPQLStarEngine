package se.liu.ida.rspqlstar.store.transform;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.reasoner.IllegalParameterException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OpWrapper {
    final private List<Var> variables = new ArrayList<>();
    final private Op op;
    private boolean isOpExtend = false;

    public Op asOp() {
        return op;
    }

    /**
     * This is a wrapper class for op, it contains method for accessing variables and selectivity in an easy manner.
     * An opWrapper can be a tree of opWrappers (e.g., in the example of an OpJoin).
     *
     * @param op
     */
    public OpWrapper(Op op) {
        this.op = op;

        if (op instanceof OpTriple) {
            extractVariables((OpTriple) op);
        } else if (op instanceof OpJoin) {
            extractVariables((OpJoin) op);
        } else if (op instanceof OpExtend) {
            extractVariables((OpExtend) op);
            isOpExtend = true;
        } else if (op instanceof OpSequence) {
            extractVariables((OpSequence) op);
        } else {
            throw new NotImplementedException("Support of op " + op.getClass() + " has not been implemented");
        }
    }

    private Triple getTriple(Op op) {
        if (op instanceof OpTriple) {
            return ((OpTriple) op).getTriple();
        } else if (op instanceof OpJoin) {
            return null;
        } else if (op instanceof OpExtend) {
            final OpExtend bind = (OpExtend) op;
            final Collection<Expr> expressions = bind.getVarExprList().getExprs().values();
            if (expressions.size() != 1) {
                throw new IllegalArgumentException("OpExtend contains multiple or zero expressions, expected one: " + op);
            }
            for (Expr expr : expressions) {
                // This only works for embedded triples
                final NodeValueNode node = (NodeValueNode) expr;
                return ((Node_Triple) (node).getNode()).get();
            }
            return null; // Can never be called
        } else {
            return null;
        }
    }

    public int calculateSelectivity(List<Var> vars) {
        if (op instanceof OpTriple) {
            return calculateSelectivity((OpTriple) op, vars);
        } else if (op instanceof OpExtend) {
            return calculateSelectivity((OpExtend) op, vars);
        }
        throw new IllegalArgumentException(op + " not supported.");
    }

    public int calculateSelectivity(OpTriple op, List<Var> vars) {
        // TODO get active graph
        return SelectivityMap.getSelectivityScore(new Quad(null, op.getTriple()), vars);
    }

    public int calculateSelectivity(OpExtend op, List<Var> vars) {
        if(true) throw new IllegalStateException("yes!");
        final Collection<Expr> expressions = op.getVarExprList().getExprs().values();
        if (expressions.size() != 1) {
            throw new IllegalArgumentException("OpExtend contains multiple or zero expressions, expected one: " + op);
        }

        if (getBindVariable(op).isVariable()) {
            // Because of query rewriting, there can only be one element in the expression list.
            final NodeValueNode node = (NodeValueNode) expressions.iterator().next();
            final Triple t = ((Node_Triple) node.getNode()).get();
            // TODO get active graph
            return SelectivityMap.getSelectivityScore(new Quad(null, t), getBindVariable(op), vars);
        } else {
            // If the the bind variable is set, then the triple pattern corresponds to the full pattern
            return SelectivityMap.getHighestSelectivity();
        }
    }

    private Var getBindVariable(OpExtend op) {
        return op.getVarExprList().getVars().get(0);
    }

    private void extractVariables(OpTriple op) {
        final Triple triple = op.getTriple();
        if (!triple.getSubject().isConcrete()) {
            variables.add((Var) triple.getSubject());
        }
        if (!triple.getPredicate().isConcrete()) {
            variables.add((Var) triple.getPredicate());
        }
        if (!triple.getObject().isConcrete()) {
            variables.add((Var) triple.getObject());
        }
    }

    private void extractVariables(OpSequence op) {
        for (Op element : op.getElements()) {
            if (element instanceof OpJoin) {
                extractVariables((OpJoin) element);
            } else if (element instanceof OpTriple) {
                extractVariables((OpTriple) element);
            } else if (element instanceof OpExtend) {
                extractVariables((OpExtend) element);
            } else if (element instanceof OpSequence) {
                extractVariables((OpSequence) element);
            } else {
                throw new NotImplementedException("support of op " + element.getClass() + " has not been implemented");
            }
        }
    }

    private void extractVariables(OpExtend op) {
        for (Expr exp : op.getVarExprList().getExprs().values()) {
            if (exp instanceof NodeValueNode) {
                final NodeValueNode node = (NodeValueNode) exp;
                final Triple triple = ((Node_Triple) node.asNode()).get();
                final Node subject = triple.getSubject();
                final Node predicate = triple.getPredicate();
                final Node object = triple.getObject();

                if (!subject.isConcrete()) {
                    variables.add((Var) subject);
                }
                if (!predicate.isConcrete()) {
                    variables.add((Var) predicate);
                }
                if (!object.isConcrete()) {
                    variables.add((Var) object);
                }
            }
        }

        variables.addAll(op.getVarExprList().getVars());
    }

    private void extractVariables(OpJoin join) {
        final Op left = join.getLeft();

        if (left instanceof OpJoin) {
            extractVariables((OpJoin) left);
        } else if (left instanceof OpTriple) {
            extractVariables((OpTriple) left);
        } else if (left instanceof OpExtend) {
            extractVariables((OpExtend) left);
        } else if (left instanceof OpSequence) {
            extractVariables((OpSequence) left);
        } else {
            throw new NotImplementedException("support of op " + left.getClass() + " has not been implemented");
        }

        if (join.getRight() instanceof OpExtend) {
            extractVariables((OpExtend) join.getRight());
        } else if (join.getRight() instanceof OpTriple) {
            extractVariables((OpTriple) join.getRight());
        } else {
            throw new IllegalParameterException("Right leaf node must either an OpTriple or OpExtend, was " + join.getRight().toString());
        }
    }

    public List<Var> getVariables() {
        return variables;
    }

    @Override
    public String toString() {
        return op.toString();
    }

    public boolean isOpExtend() {
        return isOpExtend;
    }
}
