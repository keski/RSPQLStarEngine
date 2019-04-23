package se.liu.ida.rspqlstar.sse.writers;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.pfunction.PropFuncArg;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.sse.writers.WriterBasePrefix;
import org.apache.jena.sparql.sse.writers.WriterBasePrefix.Fmt;
import org.apache.jena.sparql.sse.writers.WriterLib;
import org.apache.jena.sparql.sse.writers.WriterNode;
import org.apache.jena.sparql.sse.writers.WriterOp;
import se.liu.ida.rspqlstar.algebra.op.OpWindow;
import se.liu.ida.rspqlstar.util.MyFmtUtils;

public class MyWriterOp extends WriterOp {

    public static void output(final IndentedWriter iWriter, final Op op, final SerializationContext sCxt) {
        if (sCxt == null) {
            throw new IllegalStateException("SerializationContext cannot be null");
        }

        Fmt fmt = new Fmt() {
            public void format() {
                op.visit(new MyWriterOp.OpWriterWorker(iWriter, sCxt));
            }
        };
        WriterBasePrefix.output(iWriter, fmt, sCxt.getPrologue());
    }

    static void outputNoPrologue(IndentedWriter iWriter, Op op, SerializationContext sCxt) {
        MyWriterOp.OpWriterWorker v = new MyWriterOp.OpWriterWorker(iWriter, sCxt);
        op.visit(v);
    }

    public static class OpWriterWorker extends WriterOp.OpWriterWorker {
        private IndentedWriter out;
        private SerializationContext sContext;

        public OpWriterWorker(IndentedWriter out, SerializationContext sCxt) {
            super(out, sCxt);
            this.sContext = sCxt;
            this.out = out;
        }

        public void visit(OpPropFunc opPropFunc) {
            this.start(opPropFunc, -1);
            this.out.print(MyFmtUtils.stringForNode(opPropFunc.getProperty(), this.sContext));
            this.out.println();
            this.outputPF(opPropFunc.getSubjectArgs());
            this.out.print(" ");
            this.outputPF(opPropFunc.getObjectArgs());
            this.out.println();
            this.printOp(opPropFunc.getSubOp());
            this.finish(opPropFunc);
        }

        private void outputPF(PropFuncArg pfArg) {
            if (pfArg.isNode()) {
                WriterNode.output(this.out, pfArg.getArg(), this.sContext);
            } else {
                WriterNode.output(this.out, pfArg.getArgList(), this.sContext);
            }
        }

        public void visit(OpGraph opGraph) {
            this.start(opGraph, -1);
            this.out.println(MyFmtUtils.stringForNode(opGraph.getNode(), this.sContext));
            opGraph.getSubOp().visit(this);
            this.finish(opGraph);
        }

        public void visit(OpService opService) {
            this.start(opService, -1);
            if (opService.getSilent()) {
                this.out.println("silent ");
            }

            this.out.println(MyFmtUtils.stringForNode(opService.getService(), this.sContext));
            opService.getSubOp().visit(this);
            this.finish(opService);
        }


        /**
         * OpExt is visited for extendsions. Add new ops here to make them appear
         * in the algebra tree.
         * @param opExt
         */
        public void visit(OpExt opExt) {
            if(opExt instanceof OpWindow){
                visit((OpWindow) opExt);
            } else {
                opExt.output(this.out, this.sContext);
            }
        }

        public void visit(OpWindow opWindow){
            this.start(opWindow, -1);
            this.out.println(MyFmtUtils.stringForNode(opWindow.getNode(), this.sContext));
            opWindow.getSubOp().visit(this);
            this.finish(opWindow);
        }

        public void visit(OpLabel opLabel) {
            String x = MyFmtUtils.stringForString(opLabel.getObject().toString());
            if (opLabel.hasSubOp()) {
                this.start(opLabel, 1);
                this.out.println(x);
                this.printOp(opLabel.getSubOp());
                this.finish(opLabel);
            } else {
                this.start(opLabel, -1);
                this.out.print(x);
                this.finish(opLabel);
            }

        }

        private void start(Op op, int newline) {
            WriterLib.start(this.out, op.getName(), newline);
        }

        private void finish(Op op) {
            WriterLib.finish(this.out, op.getName());
        }

        private void printOp(Op op) {
            if (op == null) {
                WriterLib.start(this.out, "null", -2);
                WriterLib.finish(this.out, "null");
            } else {
                op.visit(this);
            }

        }
    }
}
