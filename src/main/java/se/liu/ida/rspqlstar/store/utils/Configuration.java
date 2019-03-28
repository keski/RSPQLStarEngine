package se.liu.ida.rspqlstar.store.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Configuration {
    // Dictionary and index types
    public static String nodeDictionaryType;
    public static String referenceDictionaryType;
    public static String indexType;

    public static boolean useNodeDictionaryCache;
    public static boolean useQueryHeuristics;
    public static boolean reverseBindSplitting;


    // Base URI
    public static String baseUri;

    // Data
    public static String data;
    public static String dataDirectory;

    // Queries
    public static String queries;
    public static String queryDirectory;

    // Properties
    private static Properties props;

    private static void load(String path) {
        props = new Properties();
        try {
            props.load(new FileInputStream(path));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Initialize from default configuration path.
     */
    public static void init() {
        init(System.getProperty("user.dir") + "/config.properties");
    }

    /**
     * Initialize from configuration path.
     */
    public static void init(String path) {
        load(path);

        setIndexType(getString("index-type", "TreeIndex"));
        setNodeDictionaryType(getString("node-dictionary-type", "HashNodeDictionary"));
        setReferenceDictionaryType(getString("reference-dictionary-type", "HashReferenceDictionary"));
        // Optimizations
        setUseNodeDictionaryCache(getBoolean("use-node-dictionary-cache", false));
        setUseQueryHeuristics(getBoolean("use-query-heuristic", false));
        setReverseBindSplitting(getBoolean("reverse-bind-splitting", false));
        // Data and queries
        setDataFiles(getString("data", null));
        setDataDirectory(getString("data-directory", "data/"));
        setQueries(getString("queries", null));
        setQueryDirectory(getString("query-directory", "queries/"));
        // Other
        setBaseUri(getString("base-uri", "file://base/"));
    }

    private static void setReverseBindSplitting(boolean value) {
        Configuration.reverseBindSplitting = value;
    }

    public static void setReferenceDictionaryType(String dictionary) {
        referenceDictionaryType = dictionary;
    }

    public static void setIndexType(String type){
        indexType = type;
    }

    public static void setNodeDictionaryType(String dictionary) {
        nodeDictionaryType = dictionary;
    }

    public static void setUseNodeDictionaryCache(boolean value) {
        useNodeDictionaryCache = value;
    }

    public static void setUseQueryHeuristics(boolean value) {
        useQueryHeuristics = value;
    }

    private static void setBaseUri(String baseUri) {
        Configuration.baseUri = baseUri;
    }

    public static void setDataFiles(String files) {
        data = files;
    }

    public static void setQueries(String queries) {
        Configuration.queries = queries;
    }

    public static void setQueryDirectory(String queryDirectory) {
        Configuration.queryDirectory = queryDirectory;
    }

    public static void setDataDirectory(String dataDirectory) {
        Configuration.dataDirectory = dataDirectory;
    }

    public static String getString(String key, String defaultValue){
        String value = props.getProperty(key);
        if(value != null){
            return value;
        }
        return defaultValue;
    }

    public static int getInteger(String key, int defaultValue){
        String value = props.getProperty(key);
        if(value != null){
            return Integer.parseInt(value);
        }
        return defaultValue;
    }

    public static boolean getBoolean(String key, boolean defaultValue){
        String value = props.getProperty(key);
        if(value != null){
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }
}
