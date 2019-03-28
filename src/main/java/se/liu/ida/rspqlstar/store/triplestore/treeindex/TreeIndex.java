package se.liu.ida.rspqlstar.store.triplestore.treeindex;

import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import se.liu.ida.rspqlstar.store.triplepattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.triplestore.Field;
import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;
import se.liu.ida.rspqlstar.store.triplestore.AbstractIndex;
import se.liu.ida.rspqlstar.store.triplestore.Index;

import java.util.Arrays;
import java.util.Iterator;

/**
 * The TreeIndex using a sorted set to reduce the size of the size of the indexes. The triples are ordered based on
 * their assigned fields. The TreeIndex allows an iterator to be returned over a range of values, based on a lower and
 * an upper bound.
 */

public class TreeIndex extends AbstractIndex implements Index {
    final ObjectAVLTreeSet<IdBasedQuad> index;
    private final String[] supportedIndexes = new String[]{"GSPO", "GPOS", "GOSP", "SPOG", "POSG", "OSPG"};

    public TreeIndex(Field field1, Field field2, Field field3, Field field4){
        super(field1, field2, field3, field4);
        if(!Arrays.stream(supportedIndexes).anyMatch(fields::equals)){
            throw new IllegalStateException("Index for fields " + fields + " is not supported.");
        }
        // create index with a compatible comparator for sorting
        index = new ObjectAVLTreeSet<>((ts1, ts2) -> {
            final int order;
            if (field1 == Field.S) { // GSPO
                if (ts1.graph > ts2.graph) {
                    order = 1;
                } else if (ts1.graph < ts2.graph) {
                    order = -1;
                } else if (ts1.subject > ts2.subject) {
                    order = 1;
                } else if (ts1.subject < ts2.subject) {
                    order = -1;
                } else if (ts1.predicate > ts2.predicate) {
                    order = 1;
                } else if (ts1.predicate < ts2.predicate) {
                    order = -1;
                } else if (ts1.object > ts2.object) {
                    order = 1;
                } else if (ts1.object < ts2.object) {
                    order = -1;
                } else {
                    order = 0;
                }
            } else if (field1 == Field.P) { // GPOS
                if (ts1.graph > ts2.graph) {
                    order = 1;
                } else if (ts1.graph < ts2.graph) {
                    order = -1;
                } else if (ts1.predicate > ts2.predicate) {
                    order = 1;
                } else if (ts1.predicate < ts2.predicate) {
                    order = -1;
                } else if (ts1.object > ts2.object) {
                    order = 1;
                } else if (ts1.object < ts2.object) {
                    order = -1;
                } else if (ts1.subject > ts2.subject) {
                    order = 1;
                } else if (ts1.subject < ts2.subject) {
                    order = -1;
                } else {
                    order = 0;
                }
            } else { // GOSP
                if (ts1.graph > ts2.graph) {
                    order = 1;
                } else if (ts1.graph < ts2.graph) {
                    order = -1;
                } else if (ts1.object > ts2.object) {
                    order = 1;
                } else if (ts1.object < ts2.object) {
                    order = -1;
                } else if (ts1.subject > ts2.subject) {
                    order = 1;
                } else if (ts1.subject < ts2.subject) {
                    order = -1;
                } else if (ts1.predicate > ts2.predicate) {
                    order = 1;
                } else if (ts1.predicate < ts2.predicate) {
                    order = -1;
                } else {
                    order = 0;
                }
            }
            return order;
        });
    }

    @Override
    public void add(IdBasedQuad idBasedQuad) {
        index.add(idBasedQuad);
    }

    @Override
    public long size() {
        return index.size();
    }

    @Override
    public boolean contains(QuadStarPattern pattern) {
        return false;
    }

    @Override
    public boolean contains(IdBasedQuad idBasedQuad) {
        return false;
    }

    @Override
    public Iterator<IdBasedQuad> iterator(QuadStarPattern t) {
        final Long f1 = t.isFieldConcrete(field1) ? t.getField(field1).asKey().id : null;
        final Long f2 = t.isFieldConcrete(field2) ? t.getField(field2).asKey().id : null;
        final Long f3 = t.isFieldConcrete(field3) ? t.getField(field3).asKey().id : null;
        final Long f4 = t.isFieldConcrete(field4) ? t.getField(field4).asKey().id : null;

        if(field1 == null) return iterateAll();

        final IdBasedQuad lower = makeIndexKey(f1, f2, f3, f4, Long.MIN_VALUE);
        final IdBasedQuad upper = makeIndexKey(f1, f2, f3, f4, Long.MAX_VALUE);
        return index.subSet(lower, upper).iterator();
    }

    /**
     * Indexes supported are GSPO, GPOS, GOSP, SPOG, POSG, and OSPG
     * @param field1
     * @param field2
     * @param field3
     * @param field4
     * @param defaultValue
     * @return
     */
    private IdBasedQuad makeIndexKey(Long field1, Long field2, Long field3, Long field4, long defaultValue) {
        long f1, f2, f3, f4;
        if(fields.equals("GSPO")){
            f1 = field1 == null ? defaultValue : field1;
            f2 = field2 == null ? defaultValue : field2;
            f3 = field3 == null ? defaultValue : field3;
            f4 = field4 == null ? defaultValue : field4;
        } else if(fields.equals("GPOS")){
            f1 = field1 == null ? defaultValue : field1;
            f2 = field4 == null ? defaultValue : field4;
            f3 = field2 == null ? defaultValue : field2;
            f4 = field3 == null ? defaultValue : field3;
        } else if(fields.equals("GOSP")){
            f1 = field1 == null ? defaultValue : field1;
            f2 = field3 == null ? defaultValue : field3;
            f3 = field4 == null ? defaultValue : field4;
            f4 = field2 == null ? defaultValue : field2;
        } else if(fields.equals("SPOG")){
            f1 = field4 == null ? defaultValue : field4;
            f2 = field1 == null ? defaultValue : field1;
            f3 = field2 == null ? defaultValue : field2;
            f4 = field3 == null ? defaultValue : field3;
        } else if(fields.equals("POSG")){
            f1 = field4 == null ? defaultValue : field4;
            f2 = field3 == null ? defaultValue : field3;
            f3 = field1 == null ? defaultValue : field1;
            f4 = field2 == null ? defaultValue : field2;
        } else if(fields.equals("OSPG")){
            f1 = field4 == null ? defaultValue : field4;
            f2 = field2 == null ? defaultValue : field2;
            f3 = field3 == null ? defaultValue : field3;
            f4 = field1 == null ? defaultValue : field1;
        } else {
            throw new IllegalStateException("Unrecognized index fields: " + fields);
        }
        return new IdBasedQuad(f1, f2, f3, f4);
    }

    @Override
    public Iterator<IdBasedQuad> iterateAll() {
        return index.iterator();
    }
}
