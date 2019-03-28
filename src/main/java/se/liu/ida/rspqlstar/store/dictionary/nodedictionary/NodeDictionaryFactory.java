package se.liu.ida.rspqlstar.store.dictionary.nodedictionary;

import se.liu.ida.rspqlstar.store.utils.Configuration;

public class NodeDictionaryFactory {
    static private NodeDictionary dictionary = null;

    static public NodeDictionary get() {
        if (dictionary == null) {
            init();
        }
        return dictionary;
    }

    static public void init() {
        switch (Configuration.nodeDictionaryType) {
            case "HashNodeDictionary":
                dictionary = new HashNodeDictionary();
                break;
            default:
                throw new IllegalArgumentException("Unknown dictionary type " + Configuration.nodeDictionaryType);
        }

        // If cache is active, wrap the dictionary
        if(Configuration.useNodeDictionaryCache){
            dictionary = new CachingNodeDictionary(dictionary);
        }
    }
}
