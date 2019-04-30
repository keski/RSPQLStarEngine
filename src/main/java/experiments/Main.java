package experiments;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.RDFParser;
import se.liu.ida.rdfstar.tools.parser.lang.LangTrigStar;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.RDFStream;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngine;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.util.Date;

public class Main {
    public static void main(String[] args) {
        // Start all streams
        test1();
    }

    public static void test1(){
        RSPQLStarEngine.register();
        ARQ.init();

        TimeUtil.setOffset(new Date().getTime() - 1556617861);

        RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create("" +
                "BASE <http://base/> " +
                "PREFIX ex: <http:///example.org/ontology#> " +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "+
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                "PREFIX sosa: <http://www.w3.org/ns/sosa/> " +
                "" +
                "REGISTER STREAM <output> COMPUTED EVERY PT1S " +
                "AS " +
                "SELECT * " +
                "FROM NAMED WINDOW <w/heart_rate> ON <http://stream/heart_rate> [RANGE PT5S STEP PT1S] " +
                "WHERE { " +
                "   ?a ?b ?c . " +
                "   { SELECT * WHERE { ?a ?b ?c } } " +
                "}", RSPQLStar.syntax);


        // Heart rate stream
        final RDFStream heartRate = new RDFStream("http://stream/heart_rate");

        // Create streaming dataset
        final StreamingDatasetGraph sdg = new StreamingDatasetGraph();
        sdg.registerStream(heartRate);
        // more streams...
        sdg.initForQuery(query);
        sdg.setTime(TimeUtil.getTime());

        // Load base data
        RDFParser.create()
                .base("http://base")
                .source("./data/ontology.trigs")
                .checking(false)
                .lang(LangTrigStar.TRIGSTAR)
                .parse(sdg.getBaseDataset());


        // Start all streams
        final StreamFromFile s1 = new StreamFromFile(heartRate, "data/heart_rate.trigs", 1000);
        s1.start();


        // Register query
        RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);

        // stop gracefully
        new Thread(() -> {
            TimeUtil.silentSleep(30_000);
            s1.stop();
            qexec.stopContinuousSelect();
        }).start();

        // Start querying
        qexec.execContinuousSelect(System.out);


    }

}
