package se.liu.ida.rspqlstar.store.index;

import java.util.Iterator;

public abstract class AbstractIndex implements Index {
    final public Field field1;
    final public Field field2;
    final public Field field3;
    final public Field field4;
    final public Fields fields;

    public Fields getIndexType(Field f1, Field f2, Field f3, Field f4){
        if(f1 == Field.G && f2 == Field.S && f3 == Field.P && f4 == Field.O){
            return Fields.GSPO;
        } else if(f1 == Field.G && f2 == Field.P && f3 == Field.O && f4 == Field.S){
            return Fields.GPOS;
        } else if(f1 == Field.G && f2 == Field.O && f3 == Field.S && f4 == Field.P){
            return Fields.GOSP;
        } else if(f1 == Field.S && f2 == Field.P && f3 == Field.O && f4 == Field.G){
            return Fields.SPOG;
        } else if(f1 == Field.P && f2 == Field.O && f3 == Field.S && f4 == Field.G){
            return Fields.POSG;
        }  else if(f1 == Field.O && f2 == Field.S && f3 == Field.P && f4 == Field.G){
            return Fields.OSPG;
        } else {
            throw new IllegalStateException("Unsupported fields for index: " + f1 + f2 + f3 + f4);
        }
    }


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
        fields = getIndexType(field1, field2, field3, field4);
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
