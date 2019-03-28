package se.liu.ida.rspqlstar.store.dictionary.referencedictionary;

import se.liu.ida.rspqlstar.store.utils.Configuration;

public class ReferenceDictionaryFactory {
    static private ReferenceDictionary dictionary = null;

    static public ReferenceDictionary get() {
        if (dictionary == null) {
            init();
        }
        return dictionary;
    }

    static public void init() {
        switch (Configuration.referenceDictionaryType) {
            case "HashReferenceDictionary":
                dictionary = new HashReferenceDictionary();
                break;
            default:
                throw new IllegalArgumentException("Unknown dictionary type " + Configuration.referenceDictionaryType);
        }
    }
}
