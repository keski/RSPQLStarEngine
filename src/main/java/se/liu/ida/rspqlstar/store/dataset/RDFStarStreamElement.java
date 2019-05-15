package se.liu.ida.rspqlstar.store.dataset;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionary;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.index.*;

import java.io.PrintStream;
import java.util.Date;
import java.util.Iterator;


public class RDFStarStreamElement implements StreamRDF {
    public Index index;
    public long time;
    final NodeDictionary nd = NodeDictionaryFactory.get();
    final ReferenceDictionary refT = ReferenceDictionaryFactory.get();

    public RDFStarStreamElement(Date time){
        this.time = time.getTime();
        index = new HashIndex(Field.G, Field.S, Field.P, Field.O);
    }

    @Override
    public void start() {}

    @Override
    public void triple(Triple triple) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void quad(Quad quad){
        addQuad(quad);
    }

    private IdBasedQuad addQuad(Quad quad) {
        Node graph = quad.getGraph();
        graph = graph == null ? Quad.defaultGraphNodeGenerated : graph;
        final Node subject = quad.getSubject();
        final Node predicate = quad.getPredicate();
        final Node object = quad.getObject();

        final long g = nd.addNodeIfNecessary(graph);
        final long p = nd.addNodeIfNecessary(predicate);

        final long s;
        if (subject instanceof Node_Triple) {
            final Quad q = new Quad(graph, ((Node_Triple) subject).get());
            final IdBasedQuad idBasedQuad = addQuad(q);
            s = refT.addIfNecessary(idBasedQuad.getIdBasedTriple());
        } else {
            s = nd.addNodeIfNecessary(subject);
        }

        final long o;
        if (object instanceof Node_Triple) {
            final IdBasedQuad idBasedQuad = addQuad(new Quad(graph, ((Node_Triple) object).get()));
            o = refT.addIfNecessary(idBasedQuad.getIdBasedTriple());
        } else {
            o = nd.addNodeIfNecessary(object);
        }

        final IdBasedQuad idBasedQuad = new IdBasedQuad(g, s, p, o);
        addToIndex(idBasedQuad);
        return idBasedQuad;
    }

    public void addToIndex(IdBasedQuad idBasedQuad) {
        index.add(idBasedQuad);
    }


    @Override
    public void base(String s) {}

    @Override
    public void prefix(String s, String s1) {}

    @Override
    public void finish() {}

    public void print(PrintStream out){
        out.println("TG: " + time);
        index.iterateAll().forEachRemaining(x -> { out.println(x); });
    }

    public Iterator<IdBasedQuad> iterateAll(){
        return index.iterateAll();
    }
}
