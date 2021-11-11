package com.parER.progressive.main;

/**
 * @author giovanni
 */

public class EntityComparable implements Comparable<EntityComparable> {

    private final int entitYId;
    private final Float weight;

    public EntityComparable(int id, Float w) {
        entitYId = id;
        weight = w;
    }

    public int getId() {
        return entitYId;
    }

    public Float getWeight() {
        return weight;
    }
    @Override
    public int compareTo(EntityComparable o) {
        return Float.compare(weight, o.weight);
    }
}