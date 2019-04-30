package se.liu.ida.rspqlstar.store.dataset;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

import java.io.PrintStream;
import java.util.Date;


public class TimestampedGraph implements StreamRDF {
    // TODO replace with more efficient in the case of TG
    public DatasetGraphStar dgs = new DatasetGraphStar();
    public long time;

    public TimestampedGraph(Date time){
        this.time = time.getTime();
    }

    @Override
    public void start() {
        dgs = new DatasetGraphStar();
    }

    @Override
    public void triple(Triple triple) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void quad(Quad quad) {
        dgs.add(quad);
    }

    public TimestampedGraph addQuad(Quad quad) {
        quad(quad);
        return this;
    }

    @Override
    public void base(String s) {}

    @Override
    public void prefix(String s, String s1) {}

    @Override
    public void finish() {}

    public void print(PrintStream out){
        out.println("TG: " + time);
        dgs.GSPO.iterateAll().forEachRemaining(x -> { out.println(x); });
    }
}
