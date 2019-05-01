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

        TimeUtil.setOffset(new Date().getTime() - 1556617861000L);
        System.err.println("Start at: " + TimeUtil.getTime());

        RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create("" +
                "BASE <http://base/> " +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "+
                "PREFIX ex: <http://www.example.org/ontology#> " +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                "PREFIX sosa: <http://www.w3.org/ns/sosa/> " +
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                "REGISTER STREAM <output> COMPUTED EVERY PT1S " +
                "AS " +
                "SELECT (AVG(?heartRate) AS ?avgHeartRate) (COUNT(DISTINCT ?o1) AS ?countObs) " +
                "FROM NAMED WINDOW <w/heart_rate> ON <http://stream/heart_rate> [RANGE PT5S STEP PT1S] " +
                "FROM NAMED WINDOW <w/activity> ON <http://stream/activity> [RANGE PT5S STEP PT1S] " +
                "WHERE { " +
                "    ?person1 foaf:name \"Rut\" ." +
                "    <<?person1 ex:activity ?activity>>" +
                "        ex:expectedHeartRate [ ex:upperBound ?hrUpperBound ] ." +
                "" +
                "    WINDOW <w/heart_rate> { " +
                "        GRAPH ?g1 {" +
                "            ?o1 a sosa:Observation ;" +
                "                sosa:madeBySensor ?sensor1 ;" +
                "                sosa:hasFeatureOfInterest ?person1 ;" +
                "                sosa:observedProperty <person1/heart_rate> . " +
                "            <<?o1 sosa:hasSimpleResult ?heartRate>> ex:confidence ?confidence1 ." +
                //"            FILTER(?confidence1 >= 0.9) " +
                "        }" +
                "    }" +
                "" +
                "    WINDOW <w/activity> { " +
                "        GRAPH ?g2 {" +
                "            ?o2 a sosa:Observation ;" +
                "                sosa:madeBySensor ?sensor2 ;" +
                "                sosa:hasFeatureOfInterest ?person1 ;" +
                "                sosa:observedProperty <person1/activity> ." +
                "            <<?o2 sosa:hasSimpleResult ?activity>> ex:confidence ?confidence2 ." +
                "            FILTER(?confidence2 >= 0.9) " +
                "        } " +
                "    }" +
                "} " +
                "GROUP BY ?person1 " +
                "HAVING(COUNT(DISTINCT ?activity) = 1)", RSPQLStar.syntax);


        // Heart rate stream
        final RDFStream activity = new RDFStream("http://stream/activity");
        final RDFStream heartRate = new RDFStream("http://stream/heart_rate");

        // Create streaming dataset
        final StreamingDatasetGraph sdg = new StreamingDatasetGraph();
        sdg.registerStream(activity);
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
        final StreamFromFile s1 = new StreamFromFile(activity, "data/activity.trigs", 1000);
        final StreamFromFile s2 = new StreamFromFile(heartRate, "data/heart_rate.trigs", 1000);
        final StreamFromFile s3 = new StreamFromFile(activity, "data/activity.trigs", 1000);
        final StreamFromFile s4 = new StreamFromFile(heartRate, "data/heart_rate.trigs", 1000);
        s1.start();
        s2.start();

        s3.start();
        s4.start();


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
