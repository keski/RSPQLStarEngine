package se.liu.ida.rspqlstar.store.dictionary.referencedictionary;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;
import se.liu.ida.rspqlstar.store.triple.IdFactory;

import java.util.ArrayList;
import java.util.Map;

public class HashReferenceDictionary implements ReferenceDictionary {
    final private ArrayList<IdBasedQuad> idToNodeQuad;
    final private Map<Long, IdBasedQuad> idToNodeQuadOverflow;
    final private Map<IdBasedQuad, Long> nodeQuadToId;

    public HashReferenceDictionary() {
        idToNodeQuad = new ArrayList<>();
        idToNodeQuadOverflow = new Long2ObjectOpenHashMap<>();
        nodeQuadToId = new Object2LongOpenHashMap<>();
    }

    @Override
    public IdBasedQuad getIdBasedQuad(long id) {
        final long body = IdFactory.getReferenceIdBody(id);
        if (body <= idToNodeQuad.size()) {
            return idToNodeQuad.get((int) (body - 1));
        } else {
            return idToNodeQuadOverflow.get(body);
        }
    }

    @Override
    public long addIfNecessary(IdBasedQuad idBasedQuad) {
        final Long id = getId(idBasedQuad);
        if (id != null) {
            return id;
        }

        final long newId = IdFactory.nextReferenceKeyId();
        final long body = IdFactory.getReferenceIdBody(newId);

        if (body <= idToNodeQuad.size()) {
            idToNodeQuad.set((int) (body - 1), idBasedQuad); // replace existing value
        } else if (body <= Integer.MAX_VALUE) {
            idToNodeQuad.add((int) (body - 1), idBasedQuad); // add value to end
        } else {
            idToNodeQuadOverflow.put(body, idBasedQuad); // add to overflow map
        }
        nodeQuadToId.put(idBasedQuad, newId);
        return newId;
    }

    @Override
    public Long getId(IdBasedQuad idBasedQuad) {
        return nodeQuadToId.get(idBasedQuad);
    }

    @Override
    public long size() {
        return nodeQuadToId.size();
    }

    @Override
    public void print(int limit) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Reference Dictionary\n");

        if (idToNodeQuad.size() == 0) {
            sb.append(">>> empty <<<\n");
        }
        for (int i = 1; i <= idToNodeQuad.size() && i < limit; i++) {
            final IdBasedQuad node = getIdBasedQuad(i);
            long id = i + IdFactory.REFERENCE_BIT;
            sb.append(String.format("%s (id: %s) : %s\n", i, id, node));
        }

        System.out.println(sb.toString());
    }
}
