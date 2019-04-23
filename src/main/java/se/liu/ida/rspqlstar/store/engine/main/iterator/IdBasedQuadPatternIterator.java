package se.liu.ida.rspqlstar.store.engine.main.iterator;

import org.apache.jena.sparql.engine.ExecutionContext;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.main.quadpattern.Variable;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;
import se.liu.ida.rspqlstar.store.engine.main.quadpattern.Element;
import se.liu.ida.rspqlstar.store.engine.main.quadpattern.Key;
import se.liu.ida.rspqlstar.store.engine.main.quadpattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.engine.main.SolutionMapping;

import java.util.Collections;
import java.util.Iterator;


/**
 * Iterator for quad pattern
 */
public class IdBasedQuadPatternIterator implements Iterator<SolutionMapping> {
    final private ExecutionContext execCxt;

    final private Iterator<SolutionMapping> input;
    final private QuadStarPattern pattern;
    private SolutionMapping currentInputMapping = null;
    private QuadStarPattern currentQueryPattern;
    private Iterator<? extends IdBasedQuad> currentMatches;
    private NodeDictionary nd = NodeDictionaryFactory.get();
    private DatasetGraphStar datasetGraph;

    public IdBasedQuadPatternIterator(QuadStarPattern pattern, Iterator<SolutionMapping> solMapIter, ExecutionContext execCxt) {
        this.pattern = pattern;
        this.input = solMapIter;
        this.execCxt = execCxt;
        this.datasetGraph = ((StreamingDatasetGraph) execCxt.getDataset()).getActiveDataset();
    }

    public boolean hasNext() {
        while (currentMatches == null || !currentMatches.hasNext()) {
            if (!input.hasNext() || !pattern.isMatchable()) {
                return false;
            }

            currentInputMapping = input.next();
            currentQueryPattern = substitute(pattern, currentInputMapping);
            //System.err.println(currentQueryPattern);

            // If tp contains no variables, contains is invoked instead.
            if (currentQueryPattern.isConcrete()) {
                boolean match = datasetGraph.contains(currentQueryPattern);
                if (match) {
                    currentMatches = Collections.singleton(new IdBasedQuad(
                            currentQueryPattern.graph.asKey().id,
                            currentQueryPattern.subject.asKey().id,
                            currentQueryPattern.predicate.asKey().id,
                            currentQueryPattern.object.asKey().id)).iterator();
                } else {
                    currentMatches = Collections.<IdBasedQuad>emptyList().iterator();
                }
            } else {
                currentMatches = datasetGraph.find(currentQueryPattern);
            }
        }
        return true;
    }

    public SolutionMapping next() {
        if (!hasNext()) {
            throw new IllegalStateException();
        }
        // Create the next solution mapping:
        // i) copy the mapping currently consumed from the input iterator, and
        // ii) binding the variables in the copy to the currently matching triple (currentMatch).
        final IdBasedQuad currentMatch = currentMatches.next();
        final SolutionMapping result = new SolutionMapping(currentInputMapping);

        //Add solution mappings for variables
        if (currentQueryPattern.graph.isVariable()) {
            result.set(currentQueryPattern.graph.asVariable().varId, new Key(currentMatch.graph));
        }

        if (currentQueryPattern.subject.isVariable()) {
            result.set(currentQueryPattern.subject.asVariable().varId, new Key(currentMatch.subject));
        }

        if (currentQueryPattern.predicate.isVariable()) {
            result.set(currentQueryPattern.predicate.asVariable().varId, new Key(currentMatch.predicate));
        }

        if (currentQueryPattern.object.isVariable()) {
            result.set(currentQueryPattern.object.asVariable().varId, new Key(currentMatch.object));
        }
        return result;
    }

    /**
     * Replaces each query variable in the given quad pattern that is bound to
     * a value in the given solution mapping by this value.
     */
     public QuadStarPattern substitute(QuadStarPattern quadStarPattern, SolutionMapping solMap) {
         final Element g = tryToSubstitute(quadStarPattern.graph, solMap);
         final Element s = tryToSubstitute(quadStarPattern.subject, solMap);
         final Element p = tryToSubstitute(quadStarPattern.predicate, solMap);
         final Element o = tryToSubstitute(quadStarPattern.object, solMap);
         return new QuadStarPattern(g, s, p, o);
    }

    public Element tryToSubstitute(Element el, SolutionMapping solMap){
        if(el.isVariable()) {
            final Variable var = el.asVariable();
            if (solMap.contains(var.varId)) {
                return solMap.get(var.varId);
            }
        }
        return el;
    }
}