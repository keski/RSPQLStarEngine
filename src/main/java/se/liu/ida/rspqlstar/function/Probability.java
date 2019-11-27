package se.liu.ida.rspqlstar.function;

import org.apache.commons.math3.distribution.*;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase2;
import org.apache.jena.sparql.function.FunctionBase3;
import org.apache.jena.sparql.function.FunctionFactory;
import se.liu.ida.rspqlstar.datatypes.ProbabilityDistribution;

public class Probability {
    private static final double MIN_VALUE = 0.0000001; //

    public static FunctionFactory lessThan = s -> new FunctionBase2() { // less than or equal
        @Override
        public NodeValue exec(NodeValue nodeValue1, NodeValue nodeValue2) {
            final RealDistribution distribution = getDistribution(nodeValue1);
            final double upper = nodeValue2.getDouble();
            final double probability = distribution.cumulativeProbability(upper - MIN_VALUE);
            return NodeValue.makeDecimal(probability);
        }
    };

    public static FunctionFactory lessThanOrEqual = s -> new FunctionBase2() { // less than or equal
        @Override
        public NodeValue exec(NodeValue nodeValue1, NodeValue nodeValue2) {
            final RealDistribution distribution = getDistribution(nodeValue1);
            final double upper = nodeValue2.getDouble();
            final double probability = distribution.cumulativeProbability(upper); // inclusive
            return NodeValue.makeDecimal(probability);
        }
    };

    public static FunctionFactory greaterThan = s -> new FunctionBase2() { // greater than
        @Override
        public NodeValue exec(NodeValue nodeValue1, NodeValue nodeValue2) {
            final RealDistribution distribution = getDistribution(nodeValue1);
            final double lower = nodeValue2.getDouble();
            final double probability = distribution.cumulativeProbability(lower); // inclusive
            return NodeValue.makeDecimal(1 - probability);
        }
    };

    public static FunctionFactory greaterThanOrEqual = s -> new FunctionBase2() { // greater than or equal
        @Override
        public NodeValue exec(NodeValue nodeValue1, NodeValue nodeValue2) {
            final RealDistribution distribution = getDistribution(nodeValue1);
            final double lower = nodeValue2.getDouble();
            final double probability = distribution.cumulativeProbability(lower + MIN_VALUE);
            return NodeValue.makeDecimal(1 - probability);
        }
    };

    public static FunctionFactory between = s -> new FunctionBase3() {
        @Override
        public NodeValue exec(NodeValue nodeValue1, NodeValue nodeValue2, NodeValue nodeValue3) {
            final AbstractRealDistribution distribution = (AbstractRealDistribution) getDistribution(nodeValue1);
            final double lower = nodeValue2.getDouble();
            final double upper = nodeValue3.getDouble();
            final double probability = distribution.probability(lower, upper); //  lower < x <= upper
            return NodeValue.makeDecimal(probability);
        }
    };

    public static FunctionFactory add = s -> new FunctionBase2() {
        @Override
        public NodeValue exec(NodeValue nodeValue1, NodeValue nodeValue2) {
            if(nodeValue1.isDouble() && nodeValue2.isDouble()){
                final String lexicalForm = String.format("Constant(%s)", nodeValue1.getDouble() + nodeValue2.getDouble());
                return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
            } else if(nodeValue1.isDouble()){
                return add(getDistribution(nodeValue2), nodeValue1.getDouble());
            } if(nodeValue2.isDouble()){
                return add(getDistribution(nodeValue1), nodeValue2.getDouble());
            } else {
                return add(getDistribution(nodeValue1), getDistribution(nodeValue2));
            }
        }
    };

    public static FunctionFactory subtract = s -> new FunctionBase2() {
        @Override
        public NodeValue exec(NodeValue nodeValue1, NodeValue nodeValue2) {
            if(nodeValue1.isDouble() && nodeValue2.isDouble()){
                final String lexicalForm = String.format("Constant(%s)", nodeValue1.getDouble() - nodeValue2.getDouble());
                return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
            } else if(nodeValue1.isDouble()){
                return subtract(getDistribution(nodeValue2), nodeValue1.getDouble());
            } if(nodeValue2.isDouble()){
                return subtract(getDistribution(nodeValue1), nodeValue2.getDouble());
            } else {
                return subtract(getDistribution(nodeValue1), getDistribution(nodeValue2));
            }
        }
    };

    public static RealDistribution getDistribution(NodeValue nodeValue){
        return ProbabilityDistribution.type.parse(nodeValue.asNode().getLiteralLexicalForm());
    }

    public static NodeValue add(RealDistribution x, double k) {
        if(x instanceof NormalDistribution){
            final double mean = ((NormalDistribution) x).getMean() + k;
            final double variance = x.getNumericalVariance();
            final String lexicalForm = String.format("Normal(%s, %s)", mean, variance);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else if(x instanceof UniformRealDistribution){
            final double lower = x.getSupportLowerBound() + k;
            final double upper = x.getSupportUpperBound() + k;
            final String lexicalForm = String.format("Uniform(%s, %s)", lower, upper);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else if(x instanceof TriangularDistribution){
            final double lower = x.getSupportLowerBound() + k;
            final double upper = x.getSupportUpperBound() + k;
            final double mode = x.getSupportUpperBound() + k;
            final String lexicalForm = String.format("Triangular(%s, %s, %s)", lower, upper, mode);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        }  else if(x instanceof ConstantRealDistribution){
            final double value = x.getNumericalMean();
            final String lexicalForm = String.format("Constant(%s)", value + k);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else {
            System.err.printf("Exception: subtract(%s, Double) is not a supported operation\n", x.getClass().getSimpleName());
            return null;
        }
    }

    public static NodeValue add(RealDistribution d1, RealDistribution d2) {
        if(d1 instanceof NormalDistribution && d2 instanceof NormalDistribution){
            final NormalDistribution x = (NormalDistribution) d1;
            final NormalDistribution y = (NormalDistribution) d2;
            final double mean = x.getMean() + y.getMean();
            final double variance = x.getNumericalVariance() + y.getNumericalVariance();
            final String lexicalForm = String.format("Normal(%s, %s)", mean, variance);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else if(d1 instanceof ConstantRealDistribution && d2 instanceof ConstantRealDistribution){
            final double value1 = d1.getNumericalMean();
            final double value2 = d2.getNumericalMean();
            final String lexicalForm = String.format("Constant(%s)", value1 + value2);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else {
            System.err.printf("Exception: add(%s, %s) is not a supported operation\n", d1.getClass().getSimpleName(), d2.getClass().getSimpleName());
            return null;
        }
    }

    public static NodeValue subtract(RealDistribution x, double k) {
        if(x instanceof NormalDistribution){
            final double mean = ((NormalDistribution) x).getMean() - k;
            final double variance = x.getNumericalVariance();
            final String lexicalForm = String.format("Normal(%s, %s)", mean, variance);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else if(x instanceof UniformRealDistribution){
            final double lower = x.getSupportLowerBound() - k;
            final double upper = x.getSupportUpperBound() - k;
            final String lexicalForm = String.format("Uniform(%s, %s)", lower, upper);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        }  else if(x instanceof TriangularDistribution){
            final double lower = x.getSupportLowerBound() - k;
            final double upper = x.getSupportUpperBound() - k;
            final double mode = x.getSupportUpperBound() - k;
            final String lexicalForm = String.format("Triangular(%s, %s, %s)", lower, upper, mode);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        }   else if(x instanceof ConstantRealDistribution){
            final double value = x.getNumericalMean();
            final String lexicalForm = String.format("Constant(%s)", value - k);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else {
            System.err.printf("Exception: subtract(%s, Double) is not a supported operation\n", x.getClass().getSimpleName());
            return null;
        }
    }

    public static NodeValue subtract(RealDistribution d1, RealDistribution d2) {
        if(d1 instanceof NormalDistribution && d2 instanceof NormalDistribution){
            final NormalDistribution x = (NormalDistribution) d1;
            final NormalDistribution y = (NormalDistribution) d2;
            final double mean = x.getMean() - y.getMean();
            final double variance = x.getNumericalVariance() + y.getNumericalVariance();
            final String lexicalForm = String.format("Normal(%s, %s)", mean, variance);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else  if(d1 instanceof ConstantRealDistribution && d2 instanceof ConstantRealDistribution){
            final double value1 = d1.getNumericalMean();
            final double value2 = d2.getNumericalMean();
            final String lexicalForm = String.format("Constant(%s)", value1 - value2);
            return NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } {
            System.err.printf("Exception: subtract(%s, %s) is not a supported operation\n", d1.getClass().getSimpleName(), d2.getClass().getSimpleName());
            return null;
        }
    }
}
