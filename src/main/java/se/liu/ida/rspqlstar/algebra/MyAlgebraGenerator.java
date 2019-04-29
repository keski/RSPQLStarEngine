package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.AlgebraGenerator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpAssign;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpNull;
import org.apache.jena.sparql.syntax.*;
import se.liu.ida.rspqlstar.algebra.op.OpWindow;
import se.liu.ida.rspqlstar.syntax.ElementNamedWindow;

import java.util.Deque;

public class MyAlgebraGenerator extends AlgebraGenerator {
    protected static Transform simplify = new RSPQLStarTransformSimple();

    private Op compileElementNamedWindow(ElementNamedWindow elt) {
        final Node windowNode = elt.getWindowNameNode();
        final Op sub = compileElement(elt.getElement());
        return new OpWindow(windowNode, sub);
    }

    @Override
    protected Op compileElement(Element elt) {
        if (elt instanceof ElementGroup) {
            return this.compileElementGroup((ElementGroup)elt);
        } else if (elt instanceof ElementUnion) {
            return this.compileElementUnion((ElementUnion)elt);
        } else if (elt instanceof ElementNamedGraph) {
            return this.compileElementGraph((ElementNamedGraph)elt);
        } else if (elt instanceof ElementService) {
            return this.compileElementService((ElementService)elt);
        } else if (elt instanceof ElementTriplesBlock) {
            return this.compileBasicPattern(((ElementTriplesBlock)elt).getPattern());
        } else if (elt instanceof ElementPathBlock) {
            return this.compilePathBlock(((ElementPathBlock)elt).getPattern());
        } else if (elt instanceof ElementSubQuery) {
            return this.compileElementSubquery((ElementSubQuery)elt);
        } else if (elt instanceof ElementData) {
            return this.compileElementData((ElementData)elt);
        }  else if (elt instanceof ElementNamedWindow) {
            return this.compileElementNamedWindow((ElementNamedWindow)elt);
        } else {
            return (Op)(elt == null ? OpNull.create() : this.compileUnknownElement(elt, "compile(Element)/Not a structural element: " + Lib.className(elt)));
        }
    }

    @Override
    protected Op compileOneInGroup(Element elt, Op current, Deque<Op> acc) {
        if (elt instanceof ElementAssign) {
            ElementAssign assign = (ElementAssign)elt;
            return OpAssign.assign(current, assign.getVar(), assign.getExpr());
        } else if (elt instanceof ElementBind) {
            ElementBind bind = (ElementBind)elt;
            return OpExtend.create(current, bind.getVar(), bind.getExpr());
        } else if (elt instanceof ElementOptional) {
            ElementOptional eltOpt = (ElementOptional)elt;
            return this.compileElementOptional(eltOpt, current);
        } else {
            Op op;
            if (elt instanceof ElementMinus) {
                ElementMinus elt2 = (ElementMinus)elt;
                op = this.compileElementMinus(current, elt2);
                return op;
            } else if (!(elt instanceof ElementGroup) && !(elt instanceof ElementNamedGraph) && !(elt instanceof ElementService) && !(elt instanceof ElementUnion) && !(elt instanceof ElementSubQuery) && !(elt instanceof ElementData) && !(elt instanceof ElementTriplesBlock) && !(elt instanceof ElementPathBlock) && !(elt instanceof ElementNamedWindow)) {
                if (elt instanceof ElementExists) {
                    ElementExists elt2 = (ElementExists)elt;
                    op = this.compileElementExists(current, elt2);
                    return op;
                } else if (elt instanceof ElementNotExists) {
                    ElementNotExists elt2 = (ElementNotExists)elt;
                    op = this.compileElementNotExists(current, elt2);
                    return op;
                } else if (elt instanceof ElementFilter) {
                    ElementFilter f = (ElementFilter)elt;
                    return OpFilter.filter(f.getExpr(), current);
                } else {
                    return this.compileUnknownElement(elt, "compile/Element not recognized: " + Lib.className(elt));
                }
            } else {
                op = this.compileElement(elt);
                return join(current, op);
            }
        }
    }

    @Override
    public Op compile(Element elt) {
        Op op = this.compileElement(elt);
        Op op2 = op;
        if (simplify != null) {
            op2 = simplify(op);
        }

        return op2;
    }

    protected static Op simplify(Op op) {
        return Transformer.transform(simplify, MyAlgebra.toQuadForm(op));
    }

}
