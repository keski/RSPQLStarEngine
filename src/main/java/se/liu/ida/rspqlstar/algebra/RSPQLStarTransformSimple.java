package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.algebra.op.OpExtendQuad;
import se.liu.ida.rspqlstar.algebra.op.OpWindow;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RSPQLStarTransformSimple extends TransformCopy {
    final private Logger logger = Logger.getLogger(RSPQLStarTransformSimple.class);
    final private HashMap<Node_Triple, Var> varMap = new HashMap();
    final private VarDictionary varDict = VarDictionary.get();
    final boolean reverseOpExtend = true;

    public void reset(){
        varMap.clear();
    }

    @Override
    public Op transform(final OpExt opExt) {
        if(opExt instanceof OpWindow){
            final OpWindow opWindow = (OpWindow) opExt;
            final Op op = transform(opWindow.getSubOp());
            return opWindow.copy(op);
        } else {
            return opExt;
        }
    }

    @Override
    public Op transform(final OpQuadPattern opQuadPattern) {
        Op op = null;
        for(Quad quad : opQuadPattern.getPattern()){
            final Op op2 = transform(splitQuadWithEmbeddedTriple(quad));

            if(op == null) {
                op = op2;
            } else {
                op = OpSequence.create(op, op2);
            }
        }
        return op;
    }

    public Op transform(final OpSequence opSequence) {
        Op op = null;
        for(final Op op2 : opSequence.getElements()){
            if(op == null) {
                op = transform(op2);
            } else {
                op = OpSequence.create(op, transform(op2));
            }
        }
        return op;
    }

    public Op transform(final OpFilter opFilter) {
        return OpFilter.filterBy(opFilter.getExprs(), transform(opFilter.getSubOp()));
    }

    public Op transform(final OpExtend opExtend) {
        return opExtend.copy(transform(opExtend.getSubOp()));
    }

    public Op transform(final OpJoin opJoin) {
        final Op left = transform(opJoin.getLeft());
        final Op right = transform(opJoin.getRight());
        return OpJoin.create(left, right);
    }

    private Op splitQuadWithEmbeddedTriple(final Quad quad) {
        final List<Op> ops = new ArrayList<>();

        final Node subject;
        final Node predicate = quad.getPredicate();
        final Node object;

        // subject
        if (quad.getSubject() instanceof Node_Triple) {
            final Op op = makeExtend((Node_Triple) quad.getSubject(), quad.getGraph());
            if(op != null) ops.add(op);
            subject = varMap.get(quad.getSubject());
        } else {
            subject = quad.getSubject();
        }

        // object
        if (quad.getObject() instanceof Node_Triple) {
            final Op op = makeExtend((Node_Triple) quad.getObject(), quad.getGraph());
            if(op != null) ops.add(op);
            object = varMap.get(quad.getObject());
        } else {
            object = quad.getObject();
        }

        ops.add(new OpQuad(new Quad(quad.getGraph(), subject, predicate, object)));

        // join
        Op op = null;
        for(Op op2 : ops){
            if(op2 == null){
                op = op2;
            } else {
                if(reverseOpExtend){
                    op = OpSequence.create(op2, op);
                } else {
                    op = OpSequence.create(op, op2);
                }
            }
        }

        return op;
    }

    public Op makeExtend(Node_Triple node_triple, Node graph){
        if(varMap.containsKey(node_triple)) return null;

        final NodeValueNode exp = new NodeValueNode(node_triple);
        final Var var = varDict.getFreshVariable();
        varMap.put(node_triple, var);
        OpExtend opExtend = (OpExtend) OpExtend.create(OpTable.empty(), var, exp);
        return new OpExtendQuad(opExtend, graph);
    }

    private Op transform(Op op) {
        Op op2 = op;
        if (op instanceof OpQuadPattern) {
            op2 = transform((OpQuadPattern) op2);
        } else if (op instanceof OpQuad) {
            op2 = transform((OpQuad) op2);
        } else if (op instanceof OpSequence) {
            op2 = transform((OpSequence) op);
        } else if (op instanceof OpFilter) {
            op2 = transform((OpFilter) op);
        } else if (op instanceof OpJoin) {
            op2 = transform((OpJoin) op);
        } else if (op instanceof OpExtend) {
            op2 = transform((OpExtend) op);
        }  else if (op instanceof OpExtendQuad) {
            op2 = transform((OpExtendQuad) op);
        } else {
            logger.debug("Failed to split op: " + op);
            if(op instanceof OpBGP) throw new IllegalStateException();
            op2 = op;
        }
        return op2;
    }



}
