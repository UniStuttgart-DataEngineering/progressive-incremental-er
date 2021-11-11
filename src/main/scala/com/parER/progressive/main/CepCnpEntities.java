package com.parER.progressive.main;

import gnu.trove.iterator.TIntIterator;
import org.apache.jena.ext.com.google.common.collect.MinMaxPriorityQueue;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

import java.util.*;

/**
 * @author giovanni
 *         The method orders the entities in sortedEntityHeap,
 *         meanwhile the entityComparison collects the top-1 comparison for each entity;
 */
public class CepCnpEntities extends CepCnp implements Iterator<Comparison>, AbstractProgressiveMetaBlocking {

    protected PriorityQueue<EntityComparable> sortedEntityHeap;
    protected PriorityQueue<Comparison> entityComparison;
    protected MinMaxPriorityQueue<Comparison> topKperEntity;
    protected boolean[] entityCompared;
    /*private Float[] entityThreshold;*/
    /*private Float wThreshold;*/
    protected Float entityWeights;

    public CepCnpEntities(WeightingScheme scheme, int num_profiles) {
        super(scheme, 1);
        if (fixedThreshold != 1) {
            System.out.println("error in fixing the threshold");
        }
        sortedEntityHeap = new PriorityQueue<>(num_profiles, Comparator.reverseOrder());
        entityComparison = new PriorityQueue<>(num_profiles, new MetablockingComparator());
        entityCompared = new boolean[num_profiles];
    }

    @Override
    public boolean hasNext() {
        boolean top_comparisons = counter < sortedTopComparisons.length;
        boolean comparison_not_empty = !entityComparison.isEmpty();

        if (top_comparisons || comparison_not_empty) {
            return true;
        }

        int current_id;
        while (!sortedEntityHeap.isEmpty()) {
            current_id = sortedEntityHeap.poll().getId();
            entityCompared[current_id] = true;
            getComparisonEntity(current_id);
            if (!entityComparison.isEmpty()) {
                /*if (counter == sortedTopComparisons.length) {
                    entityComparison.poll(); // the first has already been compared
                }*/
                return true;
            }
        }

        return !entityComparison.isEmpty();
    }

    @Override
    public Comparison next() {
        if (counter < sortedTopComparisons.length) {
            return sortedTopComparisons[counter++];
        } else {
            return entityComparison.poll();
        }
    }


    /*This processEntity avoid to check entityCompared[neighborId] == true*/
    @Override
    protected void processEntity(int entityId) {
        int kk = (int) (10 * Math.max(1, blockAssingments / noOfEntities));
        /*kk = noOfEntities;*/
        topKperEntity = MinMaxPriorityQueue.orderedBy(new InverseMetablockingComparator()).maximumSize(kk).create();

        validEntities.clear();
        final int[] associatedBlocks = entityIndex.getEntityBlocks(entityId, 0);
        if (associatedBlocks.length == 0) {
            return;
        }

        for (int blockIndex : associatedBlocks) {
            setNormalizedNeighborEntities(blockIndex, entityId);
            TIntIterator it = neighbors.iterator();
            while (it.hasNext()) {
                int neighborId = it.next();
                if (!entityCompared[neighborId]) {
                    if (flags[neighborId] != entityId) {
                        counters[neighborId] = 0;
                        flags[neighborId] = entityId;
                    }

                    counters[neighborId]++;
                    validEntities.add(neighborId);
                }
            }
        }
    }

    /*This processArcsEntity avoid to check entityCompared[neighbor] == true*/
    @Override
    protected void processArcsEntity(int entityId) {
        int kk = (int) (10 * Math.max(1, blockAssingments / noOfEntities));
        /*kk = noOfEntities;*/
        topKperEntity = MinMaxPriorityQueue.orderedBy(new InverseMetablockingComparator()).maximumSize(kk).create();

        validEntities.clear();
        flags = new int[noOfEntities];
        for (int i = 0; i < noOfEntities; i++) {
            flags[i] = -1;
        }

        final int[] associatedBlocks = entityIndex.getEntityBlocks(entityId, 0);
        if (associatedBlocks.length == 0) {
            return;
        }

        for (int blockIndex : associatedBlocks) {
            Float blockComparisons = cleanCleanER ? bBlocks[blockIndex].getNoOfComparisons() : uBlocks[blockIndex].getNoOfComparisons();
            setNormalizedNeighborEntities(blockIndex, entityId);
            TIntIterator it = neighbors.iterator();
            while (it.hasNext()) {
                int neighborId = it.next();
                if (!entityCompared[neighborId]) {
                    if (flags[neighborId] != entityId) {
                        counters[neighborId] = 0;
                        flags[neighborId] = entityId;
                    }

                    counters[neighborId] += 1 / blockComparisons;
                    validEntities.add(neighborId);
                }
            }
        }
    }

    protected void getComparisonEntity(int entityId) {
        entityComparison.clear();
        if (weightingScheme.equals(WeightingScheme.ARCS)) {
            processArcsEntity(entityId);
        } else {
            processEntity(entityId);
        }

        TIntIterator it = validEntities.iterator();
        while (it.hasNext()) {
            int neighborId = it.next();
            Float weight = getWeight(entityId, neighborId);
            if (weight < 0) {
                continue;
            }

            Comparison comparison = getComparison(entityId, neighborId);
            comparison.setUtilityMeasure(weight);

            topKperEntity.offer(comparison);
            /*entityComparison.add(comparison);*/
        }
        /*System.out.println("topKperEntity: " + topKperEntity.size() + " :: " + validEntities.size());*/
        entityComparison.addAll(topKperEntity);
    }

    protected void setLimits() {
        firstId = 0;
        lastId = noOfEntities;
    }

    @Override
    protected void setThreshold() {
        threshold = this.fixedThreshold;
        System.out.println("Threshold: " + threshold);
    }

    @Override
    protected void verifyValidEntities(int entityId) {
        List<Float> neighbor_weights = new LinkedList<>();
        Float max_neighbor = Float.MIN_VALUE;
        Comparison max_comparison = null;
        TIntIterator it = validEntities.iterator();
        while (it.hasNext()) {
            int neighborId = it.next();
            Float weight = getWeight(entityId, neighborId);
            if (weight < 0) {
                continue;
            }

            neighbor_weights.add(weight);

            Comparison comparison = getComparison(entityId, neighborId);
            comparison.setUtilityMeasure(weight);

            if (weight > max_neighbor) {
                max_neighbor = weight;
                max_comparison = comparison;
            }
        }
        Float max = 0.0f;
        if (neighbor_weights.size() > 1) {
            /*Collections.sort(neighbor_weights);*/
            /*entityThreshold[entityId] = 0;*/
            for (Float w : neighbor_weights) {
                entityWeights += w;
                max = Math.max(max, w);
                /*entityThreshold[entityId] = Math.max(entityThreshold[entityId], w);*/
            }
            /*entityWeights /= (neighbor_weights.size());*/
            entityWeights = max;
            if (neighbor_weights.size() == 1) {
                System.out.println("sinle neighbor");
            }
            /*entityWeights /= (neighbor_weights.size() * neighbor_weights.size());*/
            entityWeights /= (neighbor_weights.size());
            /*entityWeights = neighbor_weights.get(neighbor_weights.size() - 1);*/
            /*entityThreshold[entityId] /= 10;*/
            sortedEntityHeap.add(new EntityComparable(entityId, entityWeights));
        }
        /*wThreshold = Math.min(wThreshold, max);*/
        if (max_comparison != null) {
            topComparisons.add(max_comparison);
        }
    }

    @Override
    public String getName() {
        return "CepEntity";
    }

    public void setThreshold(int t) {
        this.fixedThreshold = t;
    }
}