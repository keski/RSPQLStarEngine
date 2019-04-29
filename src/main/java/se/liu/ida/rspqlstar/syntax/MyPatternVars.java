package se.liu.ida.rspqlstar.syntax;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.PatternVars;

import java.util.Collection;

public class MyPatternVars extends PatternVars {
    public static Collection<Var> vars(Collection<Var> s, Element element) {
        MyPatternVarsVisitor v = new MyPatternVarsVisitor(s);
        vars(element, v);

        // TODO Make this more general by extending the walker and visitor
        // Visit also the named window blocks
        ((ElementGroup) element).getElements().forEach(el -> {
            if(el instanceof ElementNamedWindow){
                vars(((ElementNamedWindow) el).getElement(), v);
            }
        });
        return s;
    }
}
