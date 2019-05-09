package se.liu.ida.rspqlstar.syntax;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.*;
import org.apache.jena.sparql.pfunction.PropFuncArg;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Based on org.apache.jena.sparql.util.VarUtils
 */

public class MyVarUtils {
    public static Set<Var> getVars(Triple triple) {
        Set<Var> x = new HashSet();
        addVarsFromTriple(x, triple);
        return x;
    }

    public static void addVarsFromTriple(Collection<Var> acc, Triple triple) {
        addVar(acc, triple.getSubject());
        addVar(acc, triple.getPredicate());
        addVar(acc, triple.getObject());
    }

    public static void addVarsFromQuad(Collection<Var> acc, Quad quad) {
        addVar(acc, quad.getGraph());
        addVar(acc, quad.getSubject());
        addVar(acc, quad.getPredicate());
        addVar(acc, quad.getObject());
    }

    public static void addVarsFromTriplePath(Collection<Var> acc, TriplePath tpath) {
        addVar(acc, tpath.getSubject());
        addVar(acc, tpath.getObject());
    }

    public static void addVar(Collection<Var> acc, Node n) {
        if (n != null) {
            if (n.isVariable()) {
                if(n.toString().startsWith("??")) return; // skip internal vars for bnodes
                acc.add(Var.alloc(n));
            }  else if(n instanceof Node_Triple){
                addVarsFromTriple(acc, ((Node_Triple) n).get());
            }
        }
    }

    public static void addVarNodes(Collection<Var> acc, Collection<Node> nodes) {
        if (nodes != null) {
            Iterator var2 = nodes.iterator();

            while(var2.hasNext()) {
                Node n = (Node)var2.next();
                addVar(acc, n);
            }
        }
    }

    public static void addVarsTriples(Collection<Var> acc, Collection<Triple> triples) {
        Iterator var2 = triples.iterator();

        while(var2.hasNext()) {
            Triple triple = (Triple)var2.next();
            addVarsFromTriple(acc, triple);
        }
    }

    public static void addVars(Collection<Var> acc, BasicPattern pattern) {
        addVarsTriples(acc, pattern.getList());
    }

    public static void addVars(Collection<Var> acc, Node graphNode, BasicPattern triples) {
        addVar(acc, graphNode);
        addVars(acc, triples);
    }

    public static void addVars(Collection<Var> acc, QuadPattern quadPattern) {
        Iterator var2 = quadPattern.getList().iterator();

        while(var2.hasNext()) {
            Quad quad = (Quad)var2.next();
            addVarsFromQuad(acc, quad);
        }

    }

    public static void addVars(Collection<Var> acc, PropFuncArg arg) {
        if (arg.isNode()) {
            addVar(acc, arg.getArg());
        } else {
            addVarNodes(acc, arg.getArgList());
        }
    }
}
