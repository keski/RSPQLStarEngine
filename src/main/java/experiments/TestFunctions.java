package experiments;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.function.FunctionRegistry;
import se.liu.ida.rdfstar.tools.parser.lang.LangTrigStar;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import se.liu.ida.rspqlstar.function.Probability;
import se.liu.ida.rspqlstar.function.ZadehLogic;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStream;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngine;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.StreamFromFile;

public class TestFunctions {

    public static void main(String[] args) {
        RSPQLStarEngine.register();
        ARQ.init();

        // Namespaces
        String rspuNS = "http://w3id.org/rsp/rspu#";
        String rspuFnNs = "http://w3id.org/rsp/rspu/fn#";
        String bnNs = "http://w3id.org/rsp/rspu/bn#";

        // Bayes
        BayesianNetwork.loadNetwork("http://example.org/bn/farida", bnNs, "src/main/resources/use-case/farida.xdsl");
        FunctionRegistry.get().put(rspuFnNs + "belief", BayesianNetwork.belief);
        FunctionRegistry.get().put(rspuFnNs + "map", BayesianNetwork.map);

        // Fuzzy logic
        FunctionRegistry.get().put(rspuFnNs + "conjunction", ZadehLogic.conjunction);
        FunctionRegistry.get().put(rspuFnNs + "disjunction", ZadehLogic.disjunction);
        FunctionRegistry.get().put(rspuFnNs + "negation", ZadehLogic.negation);
        FunctionRegistry.get().put(rspuFnNs + "implication", ZadehLogic.implication);

        // Probability distribution
        FunctionRegistry.get().put(rspuFnNs + "lessThan", Probability.lessThan);
        FunctionRegistry.get().put(rspuFnNs + "lessThanOrEqual", Probability.lessThanOrEqual);
        FunctionRegistry.get().put(rspuFnNs + "greaterThan", Probability.greaterThan);
        FunctionRegistry.get().put(rspuFnNs + "greaterThanOrEqual", Probability.greaterThanOrEqual);
        FunctionRegistry.get().put(rspuFnNs + "between", Probability.between);
        FunctionRegistry.get().put(rspuFnNs + "add", Probability.add);
        FunctionRegistry.get().put(rspuFnNs + "subtract", Probability.subtract);

        String qString = "" +
                "BASE <http://base/> \n" +
                "PREFIX : <http://example.org#> \n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n" +
                "PREFIX sosa: <http://www.w3.org/ns/sosa/> \n" +
                "PREFIX rspu: <" + rspuNS + "> \n" +
                "PREFIX rspu-fn: <" + rspuFnNs + "> \n" +
                "PREFIX bn-ns: <" + bnNs + "> \n" +
                "REGISTER STREAM <s> COMPUTED EVERY PT1S AS \n" +
                "SELECT * \n" +
                "FROM NAMED WINDOW <w> ON <http://stream.org/> [RANGE PT10S STEP PT1S] " +
                "WHERE { \n" +
                "   WINDOW <w> { " +
                "      GRAPH ?g { " +
                "          ?a ?b ?c " +
                "      }" +
                "      ?g ?x ?y . " +
                "   } " +
                "} ";
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(qString, RSPQLStar.syntax);
        // Create streaming dataset
        final StreamingDatasetGraph sdg = new StreamingDatasetGraph();
        RDFStarStream s = new RDFStarStream("http://stream.org/");
        sdg.registerStream(s);
        final StreamFromFile s1 = new StreamFromFile(s, "use-case/streams/heart.trigs", 0, 1000);
        s1.start();

        // Load base data
        RDFParser.create()
                .base("http://base/")
                .source("use-case/base.trigs")
                .checking(false)
                .lang(LangTrigStar.TRIGSTAR)
                .parse(sdg.getBaseDataset());

        // Register query
        final RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);
        // Start querying
        qexec.execContinuousSelect(System.out);
    }
}
