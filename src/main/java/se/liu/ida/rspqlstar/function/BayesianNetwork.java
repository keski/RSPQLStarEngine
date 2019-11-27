package se.liu.ida.rspqlstar.function;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.jena.sparql.function.FunctionBase;
import org.apache.jena.sparql.function.FunctionFactory;
import smile.Network;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

public class BayesianNetwork {
    public static HashMap<NodeValue, Network> bnMap = new HashMap<>();
    public static HashMap<Network, String> nsMap = new HashMap<>();

    static {
        final String os = System.getProperty("os.name").toLowerCase();
        String absPath = new File("").getAbsolutePath() + "/libs/jsmile-1.4.0-academic/";
        if(os.indexOf("mac") >= 0){
            System.load(absPath + "libjsmile.jnilib");
        } else if(os.indexOf("win") >= 0){
            System.load(absPath + "jsmile.dll");
        } else if(os.indexOf("nux") >= 0){
            System.load(absPath + "libjsmile.so");
        } else {
            System.err.println(String.format("No native jSMILE library file found for %s", os));
            System.exit(1);
        }
        System.setProperty("jsmile.native.library", absPath + "libjsmile.jnilib");
        new smile.License(
                "SMILE LICENSE a7e3e373 fd7392ec 1e81f99f " +
                "THIS IS AN ACADEMIC LICENSE AND CAN BE USED " +
                "SOLELY FOR ACADEMIC RESEARCH AND TEACHING, " +
                "AS DEFINED IN THE BAYESFUSION ACADEMIC " +
                "SOFTWARE LICENSING AGREEMENT. " +
                "Serial #: 7zo6eqbrhl3lw72hwdylxr9fa " +
                "Issued for: Robin Keskis\u00e4rkk\u00e4 (robin.keski@gmail.com) " +
                "Academic institution: Link\u00f6ping University " +
                "Valid until: 2020-04-23 " +
                "Issued by BayesFusion activation server",
                new byte[] {
                        74,45,7,-7,-52,-59,0,21,122,103,-17,-91,52,3,25,-102,
                        -121,-73,-22,-89,-32,88,-111,7,3,22,102,57,69,110,-92,66,
                        53,-63,-4,13,-64,28,4,60,-38,82,25,-51,-21,46,-122,-10,
                        -83,72,-116,-31,94,70,-52,-38,73,-47,-1,-123,98,17,-121,28
                }
        );
    }

    public static Network loadNetwork(String bnUri, String ns, String filePath) {
        System.out.println("Loading " + bnUri);
        if(!bnMap.containsKey(bnUri)){
            Network net = new Network();
            net.readFile(filePath);
            NodeValue bnId = NodeValue.makeNode(NodeFactory.createURI(bnUri));
            bnMap.put(bnId, net);
            nsMap.put(net, ns);
        }
        return bnMap.get(bnUri);
    }

    public static FunctionFactory belief = s -> new FunctionBase() {
        @Override
        public NodeValue exec(List<NodeValue> params) {
            Network bn = bnMap.get(params.get(0));
            NodeValue targetNode = params.get(1);
            NodeValue targetNodeState = params.get(2);
            List<NodeValue> evidence = params.subList(3, params.size());

            float p = (float) getOutcome(bn, targetNode, targetNodeState, evidence);
            bn.clearAllEvidence();
            return NodeValue.makeFloat(p);
        }

        @Override
        public void checkBuild(String s, ExprList exprList) {}
    };

    public static FunctionFactory map = s -> new FunctionBase() {
        @Override
        public NodeValue exec(List<NodeValue> params) {
            final Network bn = bnMap.get(params.get(0));
            final NodeValue targetNode = params.get(1);
            final List<NodeValue> evidence = params.subList(2, params.size());
            final HashMap<NodeValue, Double> outcomes = getOutcomes(bn, targetNode, evidence);

            NodeValue map = null;
            double max = 0;
            for(NodeValue outcome : outcomes.keySet()){
                if(outcomes.get(outcome) > max){
                    max = outcomes.get(outcome);
                    map = outcome;
                }
            }
            bn.clearAllEvidence();
            return map;
        }

        @Override
        public void checkBuild(String s, ExprList exprList) {}
    };

    public static double getMAP(String bn, String nodeUri, String... evidence){
        return 0;
    }

    public static double getOutcome(Network bn, NodeValue targetNode, NodeValue targetNodeState, List<NodeValue> evidence){
        return getOutcomes(bn, targetNode, evidence).get(targetNodeState);
    }

    public static HashMap<NodeValue, Double> getOutcomes(Network bn, NodeValue targetNode, List<NodeValue> evidence){
        final String ns = nsMap.get(bn);
        final String targetNodeId = encode(targetNode, ns);

        for(int i=0; i < evidence.size(); i = i + 2){
            bn.setEvidence(encode(evidence.get(i), ns), encode(evidence.get(i + 1), ns));
        }
        bn.updateBeliefs();

        final HashMap<NodeValue, Double> outcomeMap = new HashMap<>();
        double[] outcomes = bn.getNodeValue(targetNodeId);
        for(int i=0; i < outcomes.length; i++){
            String outcomeId = bn.getOutcomeId(targetNodeId, i);
            outcomeMap.put(decode(outcomeId, ns), outcomes[i]);
        }
        return outcomeMap;
    }

    public static String encode(NodeValue nodeValue, String ns) {
        if(nodeValue.isLiteral()){ // supports string or boolean
            if(nodeValue.isBoolean()){
                return "B_" + nodeValue.getNode().getLiteral().getValue().toString();
            } else if(nodeValue.isString()){
                return "S_" + nodeValue.getNode().getLiteral().getValue().toString();
            } else {
                System.err.println("Unsupported type: " + nodeValue.getNode().getLiteralDatatype());
                return null;
            }
        } else {
            return nodeValue.getNode().getURI().replace(ns, "");
        }
    }

    public static NodeValue decode(String value, String ns){
        if(value.startsWith("B_")){
            return NodeValue.makeNode(NodeFactory.createLiteral(value.substring(2), XSDDatatype.XSDboolean));
        } else if(value.startsWith("S_")){
            return NodeValue.makeNode(NodeFactory.createLiteral(value.substring(2)));
        } else {
            return NodeValue.makeNode(NodeFactory.createURI(ns + value));
        }
    }

}
