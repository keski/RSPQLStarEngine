package main;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.RSPQLQueryExecution;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngine;

public class Main {

    public static void main(String[] args) {
        RSPQLStarEngine.register();
        //RSPQLStar.init();
        //ResultSetWritersSPARQLStar.init();

        ARQ.init();

        // Limitation: Projecting Node_Triple is currently not supported

        RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create("" +
                "BASE <http://base/> " +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "+
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                "" +
                "REGISTER STREAM <output> COMPUTED EVERY PT10S " +
                "AS " +
                "SELECT ?a ?b ?c " +
                "WHERE { " +
                //"   ?a ?b ?c ." +
                //"   WINDOW :w { " +
                //"      GRAPH ?g { ?a ?b ?c } . ?a ?b ?c " +
                //"   } " +
                //"   ?sensor a <Sensor> . " +
                //"   ?sensor <hasValue> ?value . " +
                "   GRAPH <g> { <<?a ?b ?c>> <hasSource> ?source . } " +
                //"   GRAPH <g> { ?sensor <hasValue> ?value2 . } " +
                //"   FILTER (?value > 1)" +
                "}", RSPQLStar.syntax);

        // Print algebra tree
        // System.err.println(MyAlgebra.compile(query));

        StreamingDatasetGraph sdg = new StreamingDatasetGraph();
        addData(sdg);
        Dataset dataset = DatasetFactory.wrap(sdg);
        QueryExecution qexec = new RSPQLQueryExecution(query, dataset);



        ResultSet rs = qexec.execSelect();
        System.out.println("\n");
        ResultSetMgr.write(System.out, rs, ResultSetLang.SPARQLResultSetText);
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
