package se.liu.ida.rspqlstar.algebra;

import java.util.*;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprVars;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;

/**
 * Class that defines the triple pattern selectivity heuristics.
 */

public class SelectivityMap {
    static private boolean useHeuristics = false;
    static private Map<Integer, Integer> score = createMap();

    private static Map<Integer, Integer> createMap() {
        final Map<Integer, Integer> score = new HashMap<>();
        score.put(Integer.parseInt("1111"), 32); //g (s,p,o)
        score.put(Integer.parseInt("1101"), 30); //g (s,?,o)
        score.put(Integer.parseInt("1011"), 28); //g (?,p,o)
        score.put(Integer.parseInt("1110"), 26); //g (s,p,?)
        score.put(Integer.parseInt("1001"), 24); //g (?,?,o)
        score.put(Integer.parseInt("1100"), 22); //g (s,?,?)
        score.put(Integer.parseInt("1010"), 20); //g (?,p,?)
        score.put(Integer.parseInt("1000"), 18); //g (?,?,?)
        score.put(Integer.parseInt("0111"), 16); //? (s,p,o)
        score.put(Integer.parseInt("0101"), 14); //? (s,?,o)
        score.put(Integer.parseInt("0011"), 12); //? (?,p,o)
        score.put(Integer.parseInt("0110"), 10); //? (s,p,?)
        score.put(Integer.parseInt("0001"), 8);  //? (?,?,o)
        score.put(Integer.parseInt("0100"), 6);  //? (s,?,?)
        score.put(Integer.parseInt("0010"), 4);  //? (?,p,?)
        score.put(Integer.parseInt("0000"), 2);  //? (?,?,?)
        return score;
    }

    public static int getSelectivityScore(Quad quad, List<Var> vars) {
        // Return static value if heuristics is disabled
        if(!useHeuristics) return 0;

        int selectivityKey = 0;
        if (quad.getGraph() != null && quad.getGraph().isConcrete() || vars.contains(quad.getGraph())) {
            selectivityKey += 1;
        }
        if (quad.getSubject().isConcrete() || vars.contains(quad.getSubject())) {
            selectivityKey += 2;
        }
        if (quad.getPredicate().isConcrete() || vars.contains(quad.getObject())) {
            selectivityKey += 4;
        }
        if (quad.getObject().isConcrete() || vars.contains(quad.getPredicate())) {
            selectivityKey += 8;
        }

        return score.get(selectivityKey);
    }

    public static int getHighestSelectivity() {
        return score.get(Integer.parseInt("1111"));
    }

    public static int getSelectivityScore(Quad quad, Var bindVariable, List<Var> vars) {
        // Return static value if heuristics is disabled
        if(!useHeuristics){
            return 0;
        }

        if (vars.contains(bindVariable)) {
            return getHighestSelectivity();
        }
        return getSelectivityScore(quad, vars) - 1;
    }

    public static int getSelectivityScore(OpFilter opFilter, List<Var> vars) {
        // Return static value if heuristics is disabled
        if(!useHeuristics){
            System.err.println("Selectivity 0");
            return 0;
        }

        // if all vars are bound (which they should be on encountering a filter),
        // this filter should be assigned the highest selectivity
        Iterator<Expr> iter = opFilter.getExprs().iterator();
        while(iter.hasNext()){
            Expr expr = iter.next();
            Set<String> exprVars = ExprVars.getVarNamesMentioned(expr);
            for(String varName : exprVars) {
                if(!vars.contains(Var.alloc(varName))){
                    return 0;
                }
            }
        }

        return getHighestSelectivity() + 1;
    }
}
