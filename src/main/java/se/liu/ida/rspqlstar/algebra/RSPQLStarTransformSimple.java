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

public class RSPQLStarTransformSimple extends TransformCopy {
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
        Op op = null;
        for(Quad quad : opQuadPattern.getPattern()){
            Op op2;
            if(containsEmbeddedTriple(quad)){
                List<Op> splitQuadWithEmbeddedTriple(quad);
            } else {
                op2 = new OpQuad(quad);
            }

            if(op == null) {
                op = op2;
            } else {
                op = OpSequence.create(op, new OpQuad(quad));
            }
        }
        return op;
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

}
