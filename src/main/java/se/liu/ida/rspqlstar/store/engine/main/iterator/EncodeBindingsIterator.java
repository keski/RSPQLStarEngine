package se.liu.ida.rspqlstar.store.engine.main.iterator;


import java.util.Iterator;

import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;

import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.engine.main.pattern.Key;
import se.liu.ida.rspqlstar.store.engine.main.SolutionMapping;
import se.liu.ida.rspqlstar.store.index.IdBasedTriple;


/**
 * Iterator for encoding solution mappings (bindings)
 */
public class EncodeBindingsIterator implements Iterator<SolutionMapping>, Closeable {
    final private Logger logger = Logger.getLogger(EncodeBindingsIterator.class);
    final private QueryIterator input;
    final private NodeDictionary nd = NodeDictionaryFactory.get();
    final private VarDictionary varDict = VarDictionary.get();

    public EncodeBindingsIterator(QueryIterator input, ExecutionContext execCxt) {
        this.input = input;
    }

    public boolean hasNext() {
        return input.hasNext();
    }

    public SolutionMapping next() {
        final Binding curInput = input.next();

        // TODO Should we check if input is empty now?
        //if (!curInput.isEmpty()) {
        //    throw new IllegalStateException("curInput is not empty!");
        //}

        final SolutionMapping curOutput = new SolutionMapping(varDict.size());
        final Iterator<Var> iter = curInput.vars();
        while (iter.hasNext()) {
            final Var var = iter.next();
            final Node value = curInput.get(var);
            if(value instanceof Node_Triple){
                final IdBasedTriple idBasedTriple = asIdBaseTriple(((Node_Triple) value).get());
                curOutput.set(varDict.getId(var), new TripleWrapperKey(idBasedTriple));
            } else {
                final Long id = nd.getId(value);
                Key key = id != null ? new Key(id) : new NodeWrapperKey(value);
                curOutput.set(varDict.getId(var), key);
                //curOutput.set(varDict.getId(var), new Key(id));
            }
        }
        return curOutput;
    }

    private IdBasedTriple asIdBaseTriple(Triple value) {
        final long subject = nd.getId(value.getSubject());
        final long predicate = nd.getId(value.getSubject());
        final long object = nd.getId(value.getSubject());
        return new IdBasedTriple(subject, predicate, object);
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void close() {
        input.close();
    }
}
