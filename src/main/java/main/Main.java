package main;

import org.apache.jena.query.*;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.engine.QueryExecutionBase;
import se.liu.ida.rdfstar.tools.parser.lang.LangTrigStar;
import se.liu.ida.rdfstar.tools.sparqlstar.lang.SPARQLStar;
import se.liu.ida.rdfstar.tools.sparqlstar.resultset.ResultSetWritersSPARQLStar;
import se.liu.ida.rspqlstar.store.graph.DatasetStarGraph;
import se.liu.ida.rspqlstar.store.queryengine.QueryEngineStar;
import se.liu.ida.rspqlstar.store.utils.Configuration;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        final String configFile = System.getProperty("user.dir") + "/config.properties";
        Configuration.init(configFile);

        new Main().start();
    }

    public void start() throws InterruptedException {
        LangTrigStar.init();
        SPARQLStar.init();
        QueryEngineStar.register();
        ResultSetWritersSPARQLStar.init();

        // Create dataset
        DatasetStarGraph dsg = new DatasetStarGraph();
        Dataset ds = DatasetFactory.wrap(dsg);

        // Load base data
        String dataPath = System.getProperty("user.dir") + "/data/test.trigs";
        RDFParser.create()
                .base(Configuration.baseUri)
                .source(dataPath)
                .checking(false)
                .parse(dsg.getStore());

        // Execute query
        Query query = QueryFactory.create("" +
                "SELECT DISTINCT * WHERE { " +
                "   ?a ?b ?c" +
                "}", "file://base/", SPARQLStar.syntax);
        QueryExecution qexec = QueryExecutionFactory.create(query, ds);

        System.out.println(qexec.getClass());
        System.out.println(((QueryExecutionBase) qexec).getPlan());
        if(true) { return; }
        ResultSet rs = qexec.execSelect();
        org.apache.jena.sparql.engine.QueryExecutionBase q;
        String s = "";
        while (rs.hasNext()){
            s += rs.next() + "\n";
        }
        Thread.sleep(500);
        System.out.println(s);

    }

    private String readFile(String filePath){
        String content = null;
        try {
            content = new String(Files.readAllBytes(Paths.get(filePath)), Charset.forName("UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }

    /**
     * Read a results set from beginning to end. Returns null if query times out.
     * @param rs
     * @return
     */
    private String readResultSet(ResultSet rs) {
        String result = null;
        try {
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                result += qs.toString();
            }
        } catch(QueryCancelledException e){
            e.printStackTrace();
        }
        return result;
    }


    public static long getGcCount() {
        long sum = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = b.getCollectionCount();
            if (count != -1) { sum += count; }
        }
        return sum;
    }
    public static long getReallyUsedMemory() {
        long before = getGcCount();
        System.gc();
        while (getGcCount() == before);
        return getCurrentlyAllocatedMemory();
    }

    public static long getCurrentlyAllocatedMemory() {
        final Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    }
}
