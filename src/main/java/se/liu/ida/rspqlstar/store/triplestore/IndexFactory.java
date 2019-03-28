package se.liu.ida.rspqlstar.store.triplestore;

import se.liu.ida.rspqlstar.store.triplestore.treeindex.TreeIndex;
import se.liu.ida.rspqlstar.store.utils.Configuration;

public class IndexFactory {

    public static Index createIndex(Field field1, Field field2, Field field3, Field field4) {
        switch (Configuration.indexType) {
            case "TreeIndex":
                return new TreeIndex(field1, field2, field3, field4);
            default:
                throw new IllegalArgumentException("Unknown index type " + Configuration.indexType);
        }
    }
}
