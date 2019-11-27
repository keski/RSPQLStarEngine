package se.liu.ida.rspqlstar.function;

import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase1;
import org.apache.jena.sparql.function.FunctionBase2;
import org.apache.jena.sparql.function.FunctionFactory;

public class ZadehLogic {
    public static FunctionFactory conjunction = s -> new FunctionBase2() {
        @Override
        public NodeValue exec(NodeValue nodeValue1, NodeValue nodeValue2) {
            float alpha = nodeValue1.getFloat();
            float beta = nodeValue2.getFloat();
            return NodeValue.makeFloat(Math.min(alpha, beta));
        }
    };

    public static FunctionFactory disjunction = s -> new FunctionBase2() {
        @Override
        public NodeValue exec(NodeValue nodeValue1, NodeValue nodeValue2) {
            float alpha = nodeValue1.getFloat();
            float beta = nodeValue2.getFloat();
            return NodeValue.makeFloat(Math.max(alpha, beta));
        }
    };

    public static FunctionFactory negation = s -> new FunctionBase1() {
        @Override
        public NodeValue exec(NodeValue nodeValue) {
            float alpha = nodeValue.getFloat();
            return NodeValue.makeFloat(1 - alpha);
        }
    };

    public static FunctionFactory implication = s -> new FunctionBase2() {
        @Override
        public NodeValue exec(NodeValue nodeValue1, NodeValue nodeValue2) {
            float alpha = nodeValue1.getFloat();
            float beta = nodeValue2.getFloat();
            return NodeValue.makeFloat(Math.max(1- alpha, beta));
        }
    };
}
