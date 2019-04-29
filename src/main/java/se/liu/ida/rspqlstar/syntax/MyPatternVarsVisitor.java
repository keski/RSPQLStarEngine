package se.liu.ida.rspqlstar.syntax;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;
import org.apache.jena.sparql.syntax.PatternVarsVisitor;
import org.apache.jena.sparql.util.VarUtils;
import se.liu.ida.rspqlstar.store.engine.main.pattern.Element;

import java.util.Collection;
import java.util.Iterator;

public class MyPatternVarsVisitor extends PatternVarsVisitor {

    public MyPatternVarsVisitor(Collection<Var> s) {
        super(s);
    }

    public void visit(ElementTriplesBlock el) {
        Iterator iter = el.patternElts();

        while(iter.hasNext()) {
            Triple t = (Triple)iter.next();
            MyVarUtils.addVarsFromTriple(this.acc, t);
        }
    }

    public void visit(ElementPathBlock el) {
        Iterator iter = el.patternElts();

        while(iter.hasNext()) {
            TriplePath tp = (TriplePath)iter.next();
            if (tp.isTriple()) {
                MyVarUtils.addVarsFromTriple(this.acc, tp.asTriple());
            } else {
                MyVarUtils.addVarsFromTriplePath(this.acc, tp);
            }
        }
    }
}
