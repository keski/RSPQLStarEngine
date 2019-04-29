package se.liu.ida.rspqlstar.store.index;

import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadStarPattern;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * The TreeIndex using a sorted set to reduce the size of the size of the indexes. The triples are ordered based on
 * their assigned fields. The TreeIndex allows an iterator to be returned over a range of values, based on a lower and
 * an upper bound.
 *
 * TODO refT might have to start at MIN + 1
 */

public class TreeIndex extends AbstractIndex implements Index {
    final TreeSet<IdBasedQuad> index;
    //final ObjectAVLTreeSet<IdBasedQuad> index;


    public TreeIndex(Field field1, Field field2, Field field3, Field field4){
        super(field1, field2, field3, field4);

        // create index with a compatible comparator for sorting
        index = new TreeSet<>((ts1, ts2) -> {
            int order;

            if(fields == Fields.GSPO){
                order = compare(ts1.graph, ts2.graph);
                if(order != 0) return order;
                order = compare(ts1.subject, ts2.subject);
                if(order != 0) return order;
                order = compare(ts1.predicate, ts2.predicate);
                if(order != 0) return order;
                order = compare(ts1.object, ts2.object);
                if(order != 0) return order;
                return order;
            } else if(fields == Fields.GPOS){
                order = compare(ts1.graph, ts2.graph);
                if(order != 0) return order;
                order = compare(ts1.predicate, ts2.predicate);
                if(order != 0) return order;
                order = compare(ts1.object, ts2.object);
                if(order != 0) return order;
                order = compare(ts1.subject, ts2.subject);
                if(order != 0) return order;
                return order;
            } else if(fields == Fields.GOSP){
                order = compare(ts1.graph, ts2.graph);
                if(order != 0) return order;
                order = compare(ts1.object, ts2.object);
                if(order != 0) return order;
                order = compare(ts1.subject, ts2.subject);
                if(order != 0) return order;
                order = compare(ts1.predicate, ts2.predicate);
                if(order != 0) return order;
                return order;
            } else if(fields == Fields.SPOG){
                order = compare(ts1.subject, ts2.subject);
                if(order != 0) return order;
                order = compare(ts1.predicate, ts2.predicate);
                if(order != 0) return order;
                order = compare(ts1.object, ts2.object);
                if(order != 0) return order;
                order = compare(ts1.graph, ts2.graph);
                if(order != 0) return order;
                return order;
            } else if(fields == Fields.POSG){
                order = compare(ts1.predicate, ts2.predicate);
                if(order != 0) return order;
                order = compare(ts1.object, ts2.object);
                if(order != 0) return order;
                order = compare(ts1.subject, ts2.subject);
                if(order != 0) return order;
                order = compare(ts1.graph, ts2.graph);
                if(order != 0) return order;
                return order;
            } else if(fields == Fields.OSPG){
                order = compare(ts1.object, ts2.object);
                if(order != 0) return order;
                order = compare(ts1.subject, ts2.subject);
                if(order != 0) return order;
                order = compare(ts1.predicate, ts2.predicate);
                if(order != 0) return order;
                order = compare(ts1.graph, ts2.graph);
                if(order != 0) return order;
                return order;
            } else {
                throw new IllegalStateException("Unsupported fields: " + fields);
            }
        });
    }

    public int compare(long l1, long l2){
        if(l1 < l2){
            return -1;
        } else if(l1 > l2){
            return 1;
        } else {
            return 0;
        }
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
    public boolean contains(QuadStarPattern p) {
        if(!p.isConcrete()) throw new IllegalStateException("Pattern is not concrete, call iterator instead: " + p);
        final long f1 = p.getField(field1).asKey().id;
        final long f2 = p.getField(field2).asKey().id;
        final long f3 = p.getField(field3).asKey().id;
        final long f4 = p.getField(field4).asKey().id;

        final IdBasedQuad key = makeIndexKey(f1, f2, f3, f4, Long.MIN_VALUE);
        return index.contains(key);
    }

    @Override
    public boolean contains(IdBasedQuad q) {
        return index.contains(q);
    }

    @Override
    public Iterator<IdBasedQuad> iterator(QuadStarPattern p) {
        if(p.isConcrete()) throw new IllegalStateException("Patterns is concrete, call contains instead");

        final Long f1 = p.isFieldConcrete(field1) ? p.getField(field1).asKey().id : null;
        final Long f2 = p.isFieldConcrete(field2) ? p.getField(field2).asKey().id : null;
        final Long f3 = p.isFieldConcrete(field3) ? p.getField(field3).asKey().id : null;
        final Long f4 = p.isFieldConcrete(field4) ? p.getField(field4).asKey().id : null;

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
    private IdBasedQuad makeIndexKey(Long field1, Long field2, Long field3, Long field4, Long defaultValue) {
        long f1, f2, f3, f4;
        if(fields == Fields.GSPO){
            f1 = field1 == null ? defaultValue : field1;
            f2 = field2 == null ? defaultValue : field2;
            f3 = field3 == null ? defaultValue : field3;
            f4 = field4 == null ? defaultValue : field4;
        } else if(fields == Fields.GPOS){
            f1 = field1 == null ? defaultValue : field1;
            f2 = field4 == null ? defaultValue : field4;
            f3 = field2 == null ? defaultValue : field2;
            f4 = field3 == null ? defaultValue : field3;
        } else if(fields == Fields.GOSP){
            f1 = field1 == null ? defaultValue : field1;
            f2 = field3 == null ? defaultValue : field3;
            f3 = field4 == null ? defaultValue : field4;
            f4 = field2 == null ? defaultValue : field2;
        } else if(fields == Fields.SPOG){
            f1 = field4 == null ? defaultValue : field4;
            f2 = field1 == null ? defaultValue : field1;
            f3 = field2 == null ? defaultValue : field2;
            f4 = field3 == null ? defaultValue : field3;
        } else if(fields == Fields.POSG){
            f1 = field4 == null ? defaultValue : field4;
            f2 = field3 == null ? defaultValue : field3;
            f3 = field1 == null ? defaultValue : field1;
            f4 = field2 == null ? defaultValue : field2;
        } else if(fields == Fields.OSPG){
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
