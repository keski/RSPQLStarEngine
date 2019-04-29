package se.liu.ida.rspqlstar.algebra.op;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.reasoner.IllegalParameterException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprVars;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import se.liu.ida.rspqlstar.algebra.SelectivityMap;

import java.util.*;

/**
 * A wrapper class for regular opQuad. Supports easy access to variables and selectivity score.
 * An opWrapper can be a tree of opWrappers (e.g., in the example of an OpJoin).
 */

public class OpWrapper {
    final private List<Var> variables = new ArrayList<>();
    final private Op op;
    private boolean isOpExtend = false;

    public Op asOp() {
        return op;
    }


    public OpWrapper(Op op) {
        this.op = op;

        if (op instanceof OpQuad) {
            extractVariables((OpQuad) op);
        } else if (op instanceof OpJoin) {
            extractVariables((OpJoin) op);
        } else if (op instanceof OpExtend) {
            extractVariables((OpExtend) op);
            isOpExtend = true;
        } else if (op instanceof OpSequence) {
            extractVariables((OpSequence) op);
        } else if (op instanceof OpFilter) {
            extractVariables((OpFilter) op);
        } else {
            throw new NotImplementedException("Support of op " + op.getClass() + " has not been implemented");
        }
    }

    /*private Triple getTriple(Op op) {
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
    }*/

    public int calculateSelectivity(List<Var> vars) {
        if (op instanceof OpQuad) {
            return calculateSelectivity((OpQuad) op, vars);
        } else if (op instanceof OpExtend) {
            return calculateSelectivity((OpExtend) op, vars);
        } else if (op instanceof OpFilter) {
            return calculateSelectivity((OpFilter) op, vars);
        }
        //System.err.println("Heuristics for " + op.getClass() + " not supported.");
        return 0;

    }

    public int calculateSelectivity(OpQuad opQuad, List<Var> vars) {
        return SelectivityMap.getSelectivityScore(opQuad.getQuad(), vars);
    }

    public int calculateSelectivity(OpFilter opFilter, List<Var> vars) {
        return SelectivityMap.getSelectivityScore(opFilter, vars);
    }

    public int calculateSelectivity(OpExtend op, List<Var> vars) {
        //if(true) throw new IllegalStateException("yes!");
        final Collection<Expr> expressions = op.getVarExprList().getExprs().values();
        if (expressions.size() != 1) {
            throw new IllegalArgumentException("OpExtend contains multiple or zero expressions, expected one: " + op);
        }

        if (getBindVariable(op).isVariable()) {
            // Because of query rewriting, there can only be one element in the expression list.
            final NodeValueNode node = (NodeValueNode) expressions.iterator().next();
            final Triple t = ((Node_Triple) node.getNode()).get();
            return SelectivityMap.getSelectivityScore(new Quad(null, t), getBindVariable(op), vars);
        } else {
            // If the the bind variable is set, then the triple pattern corresponds to the full pattern
            return SelectivityMap.getHighestSelectivity();
        }
    }

    private Var getBindVariable(OpExtend op) {
        return op.getVarExprList().getVars().get(0);
    }

    private void extractVariables(OpQuad opQuad) {
        final Quad quad = opQuad.getQuad();
        if (!quad.getGraph().isConcrete()) {
            variables.add((Var) quad.getGraph());
        }
        if (!quad.getSubject().isConcrete()) {
            variables.add((Var) quad.getSubject());
        }
        if (!quad.getPredicate().isConcrete()) {
            variables.add((Var) quad.getPredicate());
        }
        if (!quad.getObject().isConcrete()) {
            variables.add((Var) quad.getObject());
        }
    }

    private void extractVariables(OpSequence op) {
        for (Op element : op.getElements()) {
            if (element instanceof OpJoin) {
                extractVariables((OpJoin) element);
            } else if (element instanceof OpQuad) {
                extractVariables((OpQuad) element);
            } else if (element instanceof OpExtend) {
                extractVariables((OpExtend) element);
            } else if (element instanceof OpSequence) {
                extractVariables((OpSequence) element);
            } else if (element instanceof OpFilter) {
                extractVariables((OpFilter) element);
            } else if (element instanceof OpTable) {
                // skip
            } else {
                throw new NotImplementedException(element.toString());
            }
        }
    }

    private void extractVariables(OpJoin opJoin) {
        final Op left = opJoin.getLeft();
        final Op right = opJoin.getRight();

        if (left instanceof OpJoin) {
            extractVariables((OpJoin) left);
        } else if (left instanceof OpQuad) {
            extractVariables((OpQuad) left);
        } else if (left instanceof OpExtend) {
            extractVariables((OpExtend) left);
        } else if (left instanceof OpSequence) {
            extractVariables((OpSequence) left);
        }  else if (left instanceof OpFilter) {
            extractVariables((OpFilter) left);
        } else {
            throw new NotImplementedException(left.toString());
        }

        if (right instanceof OpExtend) {
            extractVariables((OpExtend) right);
        } else if (right instanceof OpQuad) {
            extractVariables((OpQuad) right);
        }   else if (right instanceof OpFilter) {
            extractVariables((OpFilter) right);
        }else {
            throw new NotImplementedException(opJoin.getRight().toString());
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

    private void extractVariables(OpFilter opFilter) {
        final Iterator<Expr> iter = opFilter.getExprs().iterator();
        while(iter.hasNext()){
            final Expr expr = iter.next();
            final Set<String> exprVars = ExprVars.getVarNamesMentioned(expr);
            for(String varName : exprVars) {
                variables.add(Var.alloc(varName));
            }
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
