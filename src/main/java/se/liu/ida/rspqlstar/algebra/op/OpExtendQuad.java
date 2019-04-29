package se.liu.ida.rspqlstar.algebra.op;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExt;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.util.NodeIsomorphismMap;
import se.liu.ida.rspqlstar.sse.writers.MyWriterOp;

public class OpExtendQuad extends OpExt {
    private final OpExtend subOp;
    private final Node graph;

    public OpExtendQuad(OpExtend subOp, Node graph) {
        super("extendquad");
        this.subOp = subOp;
        this.graph = graph;
    }


    public OpExtend getSubOp() {
        return subOp;
    }

    public Node getGraph() {
        return graph;
    }

    @Override
    public Op effectiveOp() {
        throw new NotImplemented();
    }

    @Override
    public QueryIterator eval(QueryIterator queryIterator, ExecutionContext executionContext) {
        throw new NotImplemented();
    }

    @Override
    public void outputArgs(IndentedWriter indentedWriter, SerializationContext serializationContext) {
        throw new NotImplemented();
    }

    @Override
    public int hashCode() {
        return graph.hashCode() ^ getSubOp().hashCode();
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if (!(other instanceof OpExtendQuad)) {
            return false;
        } else {
            final OpExtendQuad opExtendQuad = (OpExtendQuad) other;
            return !graph.equals(opExtendQuad.graph) ? false : getSubOp().equalTo(opExtendQuad.getSubOp(), labelMap);
        }
    }

    public void output(IndentedWriter out, SerializationContext sCxt) {
        int line = out.getRow();
        MyWriterOp.output(out, this, sCxt);
        if (line != out.getRow()) {
            out.ensureStartOfLine();
        }
    }
}
