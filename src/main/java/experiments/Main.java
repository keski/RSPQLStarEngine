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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;

public class Main {
    public static void main(String[] args) throws IOException {
        // Start all streams
        test1();
    }

    public static void test1() throws IOException {
        RSPQLStarEngine.register();
        ARQ.init();

        TimeUtil.setOffset(new Date().getTime() - 1556617861000L);
        System.err.println("Start at: " + TimeUtil.getTime());

        // Load query
        final File file = new File(ClassLoader.getSystemClassLoader().getResource("query/query1.rspqlstar").getFile());
        final String qString = new String(Files.readAllBytes(file.toPath()));
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(qString, RSPQLStar.syntax);


        // Heart rate stream
        final RDFStream activity = new RDFStream("http://stream/activity");
        final RDFStream heartRate = new RDFStream("http://stream/heart_rate");
        final RDFStream breathing = new RDFStream("http://stream/breathing_rate");

        // Create streaming dataset
        final StreamingDatasetGraph sdg = new StreamingDatasetGraph();

        // Load base data
        RDFParser.create()
                .base("http://base")
                .source("./data/ontology.trigs")
                .checking(false)
                .lang(LangTrigStar.TRIGSTAR)
                .parse(sdg.getBaseDataset());

        // Add data stream sources
        sdg.registerStream(activity);
        sdg.registerStream(heartRate);
        sdg.registerStream(breathing);

        sdg.setTime(TimeUtil.getTime());

        // Start all streams
        final StreamFromFile s1 = new StreamFromFile(activity, "data/activity.trigs", 1000);
        final StreamFromFile s2 = new StreamFromFile(heartRate, "data/heart_rate.trigs", 1000);
        final StreamFromFile s3 = new StreamFromFile(breathing, "data/breathing_rate.trigs", 1000);
        s1.start();
        s2.start();
        s3.start();

        // Register query
        final RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);

        // stop gracefully after 1 hour
        new Thread(() -> {
            TimeUtil.silentSleep(1000 * 60 * 60);
            s1.stop();
            qexec.stopContinuousSelect();
        }).start();

        // Start querying
        qexec.execContinuousSelect(System.out);


    }

}
