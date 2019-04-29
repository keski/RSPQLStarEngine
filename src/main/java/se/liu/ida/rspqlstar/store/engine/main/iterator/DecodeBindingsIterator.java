package se.liu.ida.rspqlstar.store.engine.main.iterator;

import org.apache.jena.graph.*;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.Node_Concrete_WithID;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionary;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.main.pattern.Key;
import se.liu.ida.rspqlstar.store.dictionary.IdFactory;
import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.engine.main.SolutionMapping;
import se.liu.ida.rspqlstar.store.index.IdBasedTriple;

import java.util.Iterator;

/**
 * Iterator for decoding a solution mapping.
 */
public class DecodeBindingsIterator extends QueryIter {
    final private Iterator<SolutionMapping> input;
    final private NodeDictionary nd = NodeDictionaryFactory.get();
    final private ReferenceDictionary rd = ReferenceDictionaryFactory.get();
    final private VarDictionary varDict = VarDictionary.get();

    // initialization
    public DecodeBindingsIterator(Iterator<SolutionMapping> input, ExecutionContext execCxt) {
        super(execCxt);
        this.input = input;
    }

    protected boolean hasNextBinding() {
        return input.hasNext();
    }

    /**
     * Decode variable bindings using the node dictionary. This step is generally only required when
     * serializing the results.
     *
     * Note: org.apache.jena.sparql.util.FmtUtils will fail to serialize StarNode correctly. To avoid issues, we
     * convert nodes into Jena nodes prior serialization.
     *
     * @return
     */
    protected Binding moveToNextBinding() {
        final SolutionMapping curInput = input.next();
        final BindingHashMap curOutput = new BindingHashMap();
        for (int i = curInput.size() - 1; i >= 0; i--) {
            if (curInput.contains(i)) {
                final Key key = curInput.get(i);
                curOutput.add(varDict.getVar(i), getNode(key));
            }
        }
        return curOutput;
    }

    private Node getNode(Key key) {
        final Node node;
        try {
            if (key instanceof NodeWrapperKey) {
                // We ignore the graph node for the TripleWrapperKey, since this will
                // be implicit from the context of the Node_Triple
                final IdBasedTriple idBasedQuad = ((TripleWrapperKey) key).idBasedTriple;
                final Node s = getNode(new Key(idBasedQuad.subject));
                final Node p = getNode(new Key(idBasedQuad.predicate));
                final Node o = getNode(new Key(idBasedQuad.object));
                node = new Node_Triple(new Triple(s, p, o));
            } else {
                if (IdFactory.isReferenceId(key.id)) {
                    final IdBasedTriple idBasedTriple = rd.getIdBasedTriple(key.id);
                    final Node s = getNode(new Key(idBasedTriple.subject));
                    final Node p = getNode(new Key(idBasedTriple.predicate));
                    final Node o = getNode(new Key(idBasedTriple.object));
                    node = new Node_Triple(new Triple(s, p, o));
                } else {
                    node = nd.getNode(key.id);
                }
            }

            // If a StarNode is retrieved, convert to Jena node
            if (node instanceof Node_Concrete_WithID) {
                return ((Node_Concrete_WithID) node).asJenaNode();
            }
        } catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return node;
    }

    protected void requestCancel() {
        // Do we have to cancel the (chain of) input iterator(s) ?
        throw new UnsupportedOperationException();
    }

    protected void closeIterator() {
        if (input instanceof Closeable) {
            ((Closeable) input).close();
        }
    }

}