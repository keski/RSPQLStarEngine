package experiments;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.algebra.RSPQLStarTransform;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.RDFStream;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.dictionary.IdFactory;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngine;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.StreamFromFile;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import static se.liu.ida.rspqlstar.util.Utils.*;

public class PerformanceEvaluation {
    private static Logger logger = Logger.getLogger(PerformanceEvaluation.class);
    private static boolean log = false;

    public static void main(String[] args) throws IOException {
        RSPQLStarEngine.register();
        ARQ.init();
        TimeUtil.setOffset(new Date().getTime() - 1556617861000L);

        // warm up
        boolean warmup = false;
        if(warmup) {
            RSPQLStarTransform.putOpExtendFirst = false;
            run(10, "rdfstar", 10_000);
            run(10, "reification", 10_000);
        }

        log = true;
        logger.info("mode;#annotated;avg_time;error\n");
        long timeOutAfter = 1000 * 30; // 5 minutes, 30 (use 20 last measures)

        for(int i=1; i < 11; i++) {
            reset();
            logger.info(String.format("Reification;%s;", i));
            TimeUtil.setOffset(new Date().getTime() - 1556617861000L);
            run(i, "reification", timeOutAfter);
        }

        for(int i=1; i < 11; i++) {
            reset();
            logger.info(String.format("RDFStar;%s;", i));
            TimeUtil.setOffset(new Date().getTime() - 1556617861000L);
            run(i, "rdfstar", timeOutAfter);
        }
    }

    public static void run(int x, String suffix, long timeOutAfter) throws IOException {
        final String queryFile = String.format("performance-eval/meta-query-%s.rspqlstar", suffix);
        final String qString = readFile(queryFile);
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(qString, RSPQLStar.syntax);

        // Stream
        final RDFStream rdfStream = new RDFStream("http://stream/meta");
        // Create streaming dataset
        final StreamingDatasetGraph sdg = new StreamingDatasetGraph();
        sdg.registerStream(rdfStream);

        // Register query
        final RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);

        // Start all streams
        final String fileName = String.format("performance-eval/%s-meta-%s.trigs", x, suffix);
        final StreamFromFile stream = new StreamFromFile(rdfStream, fileName, 100);
        stream.start();

        // stop gracefully
        new Thread(() -> {
            TimeUtil.silentSleep(timeOutAfter);
            stream.stop();
            qexec.stopContinuousSelect();
        }).start();

        // Start query
        PrintStream ps = new PrintStream(new ByteArrayOutputStream());
        // ps = System.out;
        qexec.execContinuousSelect(ps);

        if(log && qexec.expResults.size() > 10) {
            long[] results = asArray(qexec.expResults);
            results = Arrays.copyOfRange(results, 10, results.length);
            logger.info(calculateMean(results)/1000_000.0);
            logger.info(";");
            logger.info(calculateStandardDeviation(results)/1000_000.0);
            logger.info("\n");
        }
    }

    /**
     * Reset storage to original state (clear everything).
     */
    public static void reset(){
        NodeDictionaryFactory.get().clear();
        IdFactory.reset();
        VarDictionary.reset();
        // used make sure gc has been run
        System.err.println("Memory allocated: " + getReallyUsedMemory() + " mb");
    }

    public static long[] asArray(ArrayList<Long> list){
        long[] array = new long[list.size()];
        for(int i=0; i<list.size(); i++){
            array[i] = list.get(i);
        }
        return array;
    }
}
