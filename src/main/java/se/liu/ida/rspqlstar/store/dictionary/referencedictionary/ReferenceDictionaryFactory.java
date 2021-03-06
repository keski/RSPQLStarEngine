package se.liu.ida.rspqlstar.store.dictionary.referencedictionary;

public class ReferenceDictionaryFactory {
    static private ReferenceDictionary singleton = new HashReferenceDictionary();

    static public ReferenceDictionary get() {
        if (singleton == null) init();
        return singleton;
    }

    static public void init() {
        singleton = new HashReferenceDictionary();
    }
}
