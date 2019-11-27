package se.liu.ida.rspqlstar.function;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class bayes extends FunctionBase {

    public bayes() {
        super();
        System.out.println("init");
    }

    @Override
    public NodeValue exec(List<NodeValue> list) {
        System.out.println("inside here -> ");
        final Node h = list.get(0).asNode();
        final ArrayList<Evidence> evidence = new ArrayList<>();
        /*for(int i = 1; i < list.size(); i=i+2){
            Node n = list.get(i).asNode();
            float p = ((BigDecimal) list.get(i+1).asNode().getLiteral().getValue()).floatValue();
            evidence.add(new Evidence(n,p));
        }

        // Magic bayes
        float prob = 1;
        for(Evidence e : evidence){
            prob *= e.p;
        }*/

        String x = "Good=0.1;Bad=0.2;Okay=0.7";
        return NodeValue.makeString(x);
    }

    @Override
    public void checkBuild(String s, ExprList exprList) {}

    public class Evidence {
        Node n;
        float p;

        public Evidence(Node n, float p){
            this.n = n;
            this.p = p;
        }
    }
}