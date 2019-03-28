package se.liu.ida.rspqlstar.store.triplepattern;

/**
 * A variable as part of a triple star pattern.
 */
public class Variable extends Element {
    public final int varId;

    public Variable(int varId) {
        this.varId = varId;
    }

    @Override
    public boolean isVariable() {
        return true;
    }

    @Override
    public String toString() {
        return "#" + String.valueOf(varId);
    }

    @Override
    public boolean equals(Object o) {
        if (o != null && o instanceof Variable) {
            return varId == ((Variable) o).varId;
        }
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return varId;
    }

}
