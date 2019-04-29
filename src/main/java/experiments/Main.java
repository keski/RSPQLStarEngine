package experiments;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCheck;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.RDFStream;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.dataset.TimestampedGraph;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngine;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadStarPattern;

import java.util.*;

public class Main {
    static String base = "http://base/";

    public static void main(String[] args) {
        RSPQLStarEngine.register();
        ARQ.init();

        RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create("" +
                "BASE <http://base/> " +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "+
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                "" +
                "REGISTER STREAM <output> COMPUTED EVERY PT1S " +
                "AS " +
                "SELECT (COUNT(DISTINCT ?g) AS ?count) " +
                "FROM NAMED WINDOW <w> ON <breathing> [RANGE PT5S STEP PT1S] " +
                "WHERE { " +
                "   WINDOW <w> { " +
                "      GRAPH ?g { ?a ?b ?c } " +
                //"      ?g ?hasTime ?time . " +
                "   } " +
                //"?a ?b ?c ." +
                //"   ?sensor a <Sensor> . " +
                //"   ?sensor <hasValue> ?value . " +
                //"   GRAPH <g> { ?sensor ?b ?value . } " +
                //"   GRAPH ?g { <<?a ?b ?c>> <hasSource> ?source . } " +
                //"   FILTER (?c > 0)" +
                "}", RSPQLStar.syntax);

        // Add data

        // Breathing stream
        final RDFStream breathing = new RDFStream("http://base/breathing");
        new Thread(new BreathingStream(breathing, 100)).start();

        // Map of streams
        Map<String, RDFStream> rdfStreams = new HashMap<>();
        rdfStreams.put(breathing.iri, breathing);

        // Defined streaming dataset
        StreamingDatasetGraph sdg = new StreamingDatasetGraph(query, rdfStreams);
        sdg.setTime(new Date());

        // Create execution
        RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);

        // add data
        addData(sdg);

        // execute continuous select
        qexec.execContinuousSelect(System.out);
        //qexec.execContinuousSelectTest(System.out);

        //ResultSet rs = qexec.execSelect();
        //ResultSetMgr.write(System.out, rs, ResultSetLang.SPARQLResultSetText);
    }

    private static void addData(StreamingDatasetGraph dsg) {
        String base = "http://base/";
        Node sensor1 = ResourceFactory.createResource(base + "sensor1").asNode();
        Node sensor = ResourceFactory.createResource(base + "Sensor").asNode();
        Node hasValue = ResourceFactory.createProperty(base + "hasValue").asNode();

        Node hasSource = ResourceFactory.createProperty(base + "hasSource").asNode();
        Node producer = ResourceFactory.createProperty(base + "wikipedia").asNode();

        Node g0 = Quad.defaultGraphNodeGenerated;
        Node g1 = ResourceFactory.createResource(base + "g").asNode();
        dsg.add(new Quad(g0, sensor1, RDF.type.asNode(), sensor));

        for(int i=0; i < 10; i++) {
            dsg.add(new Quad(g0, sensor1, hasValue, ResourceFactory.createTypedLiteral(i).asNode()));
        }

        for(int i=0; i < 10; i++) {
            Triple triple = new Triple(sensor1, hasValue, ResourceFactory.createTypedLiteral(i*100).asNode());
            dsg.add(new Quad(g1, triple));
            dsg.add(new Quad(g1, new Node_Triple(triple), hasSource, producer));
        }
    }

}
