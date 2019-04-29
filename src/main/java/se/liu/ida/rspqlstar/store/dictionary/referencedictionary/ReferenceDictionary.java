package se.liu.ida.rspqlstar.store.dictionary.referencedictionary;

import se.liu.ida.rspqlstar.store.index.IdBasedTriple;

import java.io.PrintStream;

public interface ReferenceDictionary {

    IdBasedTriple getIdBasedTriple(long id);

    long addIfNecessary(IdBasedTriple idBasedTriple);

    Long getId(IdBasedTriple idBasedTriple);

    long size();

    void print(PrintStream out, int limit);
}
