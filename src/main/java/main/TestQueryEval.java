package main;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.Quad;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;
import se.liu.ida.rspqlstar.store.dataset.RDFStream;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.dataset.TimestampedGraph;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngine;

import java.util.Random;

public class TestQueryEval {
    static Random r = new Random(0);

    public static void main(String[] args) {
        RSPQLStarEngine.register();
        ARQ.init();

        RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create("" +
                "PREFIX : <http://example.org#> " +
                "REGISTER STREAM :output COMPUTED EVERY PT10S " +
                "AS " +
                "SELECT * " +
                "FROM NAMED WINDOW :w ON :s [RANGE PT10S STEP PT5S] " +
                "WHERE { " +
                "   WINDOW :w { " +
                "      GRAPH ?g { ?a ?b ?c } " +
                //"      ?a ?b ?c ." +
                "   } " +
                //"   GRAPH ?g2 { ?d ?e ?f } " +
                //"   ?g ?h ?j ." +
                "}", RSPQLStar.syntax);

        StreamingDatasetGraph sdg = new StreamingDatasetGraph();
        Dataset dataset = DatasetFactory.wrap(sdg);

        QueryExecution qexec = QueryExecutionFactory.create(query, dataset);

        ResultSet rs = qexec.execSelect();

/*
        long t0 = System.currentTimeMillis();

        StreamingDatasetGraph sdg = new StreamingDatasetGraph();
        // load base data
        String dataPath = "./data/small-yago.ttls";
        RDFParser.create()
                .base(Configuration.baseUri)
                .source(dataPath)
                .checking(false)
                .parse(sdg.getBaseDataset());

        // set up streams
        RDFStream stream1 = new RDFStream("s1");
        RDFStream stream2 = new RDFStream("s2");
        runStream(stream1, 10);
        runStream(stream2, 50);

        // windows over streams
        WindowDatasetGraph window1 = new WindowDatasetGraph("w1", 10000, 5000, t0, stream1);
        WindowDatasetGraph window2 = new WindowDatasetGraph("w2", 5000, 1000, t0, stream2);
        sdg.addWindow(window1);
        sdg.addWindow(window2);

        // Query execution
        long computeEvery = 1000;
        while(true) {
            Thread.sleep(computeEvery);
            sdg.setTime(System.currentTimeMillis());

            int count = 0;
            Iterator<IdBasedQuad> iter = sdg.iterator();
            while(iter.hasNext()){
                iter.next();
                count++;
            }
            System.out.printf("Total: %s quads\n", count);

        }
        */
    }

    private static void runStream(RDFStream stream, long delay) {
        new Thread(() -> {
            while(true) {
                long time = System.currentTimeMillis();
                TimestampedGraph tg = new TimestampedGraph(time);


                Node g = makeBnode();
                Triple t = new Triple(makeBnode(), makeProperty("value"), makeLiteral(r.nextInt()));
                Triple meta = new Triple(new Node_Triple(t), makeProperty("time"), makeLiteral(time));
                tg.quad(new Quad(g, t));
                tg.quad(new Quad(Quad.defaultGraphIRI, meta));
                stream.add(tg);

                // delay
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static void main2(String[] args){
        RSPQLStarEngine.register();
        ARQ.init();

        // Create dataset
        org.apache.jena.sparql.core.DatasetGraph baseDataset = new DatasetGraphStar();

        // Load base data
        String dataPath = "./data/test.trigs";
        RDFParser.create()
                .base("file://base/")
                .source(dataPath)
                .checking(false)
                .parse(baseDataset);
    }



    public static Node makeBnode(){
        return ResourceFactory.createResource().asNode();
    }

    public static Node makeProperty(String value){
        return ResourceFactory.createResource(value).asNode();
    }

    public static Node makeLiteral(Object value){
        return ResourceFactory.createTypedLiteral(value).asNode();
    }
}
