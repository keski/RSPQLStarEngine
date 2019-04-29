package se.liu.ida.rspqlstar.store.dictionary.nodedictionary;

public class NodeDictionaryFactory {
    static private NodeDictionary singleton = new HashNodeDictionary();

    static public NodeDictionary get() {
        if (singleton == null) init();
        return singleton;
    }

    static public void init() {
        singleton = new HashNodeDictionary();
    }
}
