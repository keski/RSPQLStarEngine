package se.liu.ida.rspqlstar.datatypes;

import org.apache.commons.math3.distribution.*;
import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.graph.impl.LiteralLabel;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProbabilityDistribution extends BaseDatatype {
    public static final String datatypeUri = "http://w3id.org/rsp/rspu#distribution";

    public static final ProbabilityDistribution type = new ProbabilityDistribution();
    private static final Pattern p = Pattern.compile("^(.*)\\((.*)\\)$");
    private static Map<String, MyFunctionalInterface> distributionMap = new HashMap<>();
    private String lexicalForm;

    static {
        distributionMap.put("Uniform", ProbabilityDistribution::getUniformDistribution);
        distributionMap.put("Triangular", ProbabilityDistribution::getTriangularDistribution);
        distributionMap.put("Constant", ProbabilityDistribution::getConstantRealDistribution);
        distributionMap.put("Normal", ProbabilityDistribution::getNormalDistribution);
    }

    @FunctionalInterface
    public interface MyFunctionalInterface {
        RealDistribution create(double... args);
    }

    private static NormalDistribution getNormalDistribution(double... args) {
        double mean = args[0];
        double variance = args[1];
        return new NormalDistribution(mean, Math.sqrt(variance));
    }

    private static UniformRealDistribution getUniformDistribution(double... args) {
        double lower = args[0];
        double upper = args[1];
        return new UniformRealDistribution(lower, upper);
    }

    private static TriangularDistribution getTriangularDistribution(double... args) {
        double lower = args[0];
        double upper = args[1];
        double mode = args[2];
        return new TriangularDistribution(lower, upper, mode);
    }

    private static ConstantRealDistribution getConstantRealDistribution(double... args) {
        double k = args[0];
        return new ConstantRealDistribution(k);
    }

    public ProbabilityDistribution() {
        super(datatypeUri);
    }

    public ProbabilityDistribution(String lexicalForm) {
        super(datatypeUri);
        this.lexicalForm = lexicalForm;
    }

    /**
     * Parse a lexical form of this datatype to a value.
     * @throws DatatypeFormatException if the lexical form is not legal
     */
    public RealDistribution parse(String lexicalForm) throws DatatypeFormatException {
        final Matcher m = p.matcher(lexicalForm);
        m.matches();
        try {
            final String d = m.group(1);
            final MyFunctionalInterface ref = distributionMap.get(d);
            final String[] split = m.group(2).split(",");
            final double[] args = new double[split.length];
            for(int i=0; i<args.length; i++){
                args[i] = Double.parseDouble(split[i]);
            }
            return ref.create(args);
        } catch(Exception e){
            System.err.println(e);
            throw new DatatypeFormatException(e.getMessage());
        }
    }

    /**
     * Compares two instances of values of the given datatype.
     * This does not allow rationals to be compared to other number
     * formats, Lang tag is not significant.
     */
    public boolean isEqual(LiteralLabel value1, LiteralLabel value2) {
        return value1.getDatatype() == value2.getDatatype() && value1.getLexicalForm().equals(value2.getLexicalForm());
    }

    public String toString() {
        return String.format("\"%s\"^^<%s>", lexicalForm, uri);
    }


}
