package se.liu.ida.rspqlstar.store.triplestore;

import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;

import java.util.Iterator;

public abstract class AbstractIndex implements Index {
    final public Field field1;
    final public Field field2;
    final public Field field3;
    final public Field field4;
    final public String fields;

    /**
     * Instantiate a new index for a set of fields.
     *
     * @param field1
     * @param field2
     * @param field3
     * @param field4
     */
    public AbstractIndex(Field field1, Field field2, Field field3, Field field4) {
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
        this.field4 = field4;
        fields = String.format("%s%s%s%s", field1, field2, field3, field4);
    }

    @Override
    public void print(int limit) {
        System.out.printf("Index: %s%s%s%s\n", field1, field2, field3, field4);
        int i = 0;
        Iterator<IdBasedQuad> iter = iterateAll();
        while(iter.hasNext() && i < limit){
            System.out.println(iter.next());
            i++;
        }
    }
}
