package se.liu.ida.rspqlstar.algebra.op;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.op.OpExt;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.util.NodeIsomorphismMap;
import se.liu.ida.rspqlstar.algebra.RSPQLStarTransform;
import se.liu.ida.rspqlstar.sse.writers.MyWriterOp;

public class OpWindow extends OpExt {

    private final Node node;
    private Op subOp;

    public OpWindow(Node name, Op subOp) {
        super("window");
        this.node = name;
        this.subOp = subOp;
    }

    public Node getNode(){
        return node;
    }

    public Op effectiveOp(){
        return subOp;
    }

    public QueryIterator eval(QueryIterator var1, ExecutionContext var2){
        throw new  NotImplemented();
    }

    public Op getSubOp(){
        return subOp;
    }

    public OpWindow copy(Op newOp) {
        return new OpWindow(node, newOp);
    }

    @Override
    public int hashCode() {
        return node.hashCode() ^ getSubOp().hashCode();
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if (!(other instanceof OpWindow)) {
            return false;
        } else {
            OpWindow opWindow = (OpWindow) other;
            return !node.equals(opWindow.node) ? false : getSubOp().equalTo(opWindow.getSubOp(), labelMap);
        }
    }

    public void output(IndentedWriter out, SerializationContext sCxt) {
        int line = out.getRow();
        MyWriterOp.output(out, this, sCxt);
        if (line != out.getRow()) {
            out.ensureStartOfLine();
        }
    }

    public Op apply(Transform transform, OpVisitor before, OpVisitor after) {
        return apply(transform);
    }

    public Op apply(Transform transform) {
        subOp = Algebra.toQuadForm(subOp);
        return this;
    }

    public void outputArgs(IndentedWriter var1, SerializationContext var2){
        throw new NotImplemented();
    };
}
