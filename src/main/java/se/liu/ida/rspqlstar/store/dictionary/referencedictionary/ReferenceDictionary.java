package se.liu.ida.rspqlstar.store.dictionary.referencedictionary;

import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;

public interface ReferenceDictionary {

    IdBasedQuad getIdBasedQuad(long id);

    long addIfNecessary(IdBasedQuad idBasedQuad);

    Long getId(IdBasedQuad idBasedQuad);

    long size();

    void print(int limit);
}
