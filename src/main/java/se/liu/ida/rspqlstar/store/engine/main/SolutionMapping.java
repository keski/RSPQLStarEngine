package se.liu.ida.rspqlstar.store.engine.main;

import se.liu.ida.rspqlstar.store.engine.main.pattern.Key;

/**
 * This class represents the solution mappings (variable bindings)
 */
public class SolutionMapping {
    final private Key[] map;
    final static private Key UNBOUND = null;


    // initialization
    public SolutionMapping(int size) {
        map = new Key[size];
        for (int i = 0; i < size; ++i) {
            map[i] = SolutionMapping.UNBOUND;
        }
    }

    public SolutionMapping(SolutionMapping template) {
        final int size = template.map.length;
        map = new Key[size];
        for (int i = 0; i < size; ++i) {
            map[i] = template.map[i];
        }
    }

    // implementation of the SolutionMapping interface
    public void set(int varId, Key key) {
        map[varId] = key;
    }

    public boolean contains(int varId) {
        return map[varId] != SolutionMapping.UNBOUND;
    }

    public Key get(int varId) {
        return map[varId];
    }

    public int size() {
        return map.length;
    }

    @Override
    public String toString() {
        String s = "SolutionMapping(";
        for (int i = 0; i < map.length; ++i) {
            if (map[i] != SolutionMapping.UNBOUND) {
                Key key = map[i];
                s += "#" + String.valueOf(i) + "->" + key.toString() + ", ";
            }
        }

        s += ")";
        return s;
    }
}
