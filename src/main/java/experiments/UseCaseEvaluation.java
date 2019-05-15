package experiments;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.RDFParser;
import se.liu.ida.rdfstar.tools.parser.lang.LangTrigStar;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStream;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngine;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.StreamFromFile;
import se.liu.ida.rspqlstar.util.TimeUtil;
import se.liu.ida.rspqlstar.util.Utils;

import java.io.IOException;
import java.util.Date;

public class UseCaseEvaluation {
    public static void main(String[] args) throws IOException {
        // Start all streams
        run("use-case/rdfstar/");
        //run("use-case/reification/");
        //test();
    }

    public static void run(String dir) throws IOException {
        RSPQLStarEngine.register();
        ARQ.init();

        TimeUtil.setOffset(new Date().getTime() - 1556617861000L);

        // Load query
        final String qString = Utils.readFile(dir + "/query.rspqlstar");
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(qString, RSPQLStar.syntax);

        final RDFStarStream activity = new RDFStarStream("http://base/s/activity");
        final RDFStarStream heart = new RDFStarStream("http://base/s/heart");
        final RDFStarStream breathing = new RDFStarStream("http://base/s/breathing");
        final RDFStarStream oxygen = new RDFStarStream("http://base/s/oxygen");
        final RDFStarStream location = new RDFStarStream("http://base/s/location");

        // Create streaming dataset
        final StreamingDatasetGraph sdg = new StreamingDatasetGraph();

        // Load base data
        RDFParser.create()
                .base("http://base/")
                .source("use-case/base-data.ttl")
                .checking(false)
                .lang(LangTrigStar.TRIGSTAR)
                .parse(sdg.getBaseDataset());
        // Add data stream sources
        sdg.registerStream(activity);
        sdg.registerStream(heart);
        sdg.registerStream(breathing);
        sdg.registerStream(oxygen);
        sdg.registerStream(location);

        sdg.setTime(TimeUtil.getTime());

        // Start all streams
        final StreamFromFile s1 = new StreamFromFile(activity, dir + "activity.trigs", 0, 10000);
        final StreamFromFile s2 = new StreamFromFile(heart, dir + "heart.trigs", 0, 1000);
        final StreamFromFile s3 = new StreamFromFile(breathing, dir + "breathing.trigs", 0, 1000);
        final StreamFromFile s4 = new StreamFromFile(oxygen, dir + "oxygen.trigs", 0, 1000);
        final StreamFromFile s5 = new StreamFromFile(location, dir + "location.trigs", 0, 10000);
        s1.start();
        s2.start();
        s3.start();
        s4.start();
        s5.start();

        // Register query
        final RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);

        // stop gracefully after 30 min
        new Thread(() -> {
            TimeUtil.silentSleep(1000 * 60 * 5);
            s1.stop();
            qexec.stopContinuousSelect();
        }).start();

        // Start querying
        qexec.execContinuousSelect(System.out);
    }

}
