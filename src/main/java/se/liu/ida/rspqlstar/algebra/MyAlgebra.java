package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.syntax.Element;

public class MyAlgebra extends Algebra {
    public static Op compile(Query query) {
        return query == null ? null : (new MyAlgebraGenerator()).compile(query);
    }
    //public static Op compile(Element elt) { return elt == null ? null : (new MyAlgebraGenerator()).compile(elt); }
}
