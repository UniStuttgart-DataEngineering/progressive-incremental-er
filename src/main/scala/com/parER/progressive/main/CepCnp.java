/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */
package com.parER.progressive.main;


// com.google.common.collect.MinMaxPriorityQueue;
import gnu.trove.iterator.TIntIterator;
import org.apache.jena.ext.com.google.common.collect.MinMaxPriorityQueue;
import org.scify.jedai.blockprocessing.comparisoncleaning.CardinalityEdgePruning;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.prioritization.IPrioritization;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

import java.util.*;

/**
 * @author gap2
 * @author giovanni
 */
public class CepCnp extends CardinalityEdgePruning implements Iterator<Comparison>, AbstractProgressiveMetaBlocking, IPrioritization {

    protected final static Float FILTER_RATIO = 0.80f;
    protected final static Float SMOOTH_FACTOR = 1.005f;

    protected int counter;
    protected int firstId;
    protected int lastId;

    protected int fixedThreshold;

    protected Comparison[] sortedTopComparisons;
    protected final Set<Comparison> topComparisons;
    protected List<AbstractBlock> blocks;

    public CepCnp(List<EntityProfile>[] profiles, WeightingScheme scheme) {
        super(scheme);
        //super("CEP+CNP", scheme);
        counter = 0;
        nodeCentric = true;
        topComparisons = new HashSet<>();
        this.fixedThreshold = 0;

        buildBlocks(profiles);
        this.applyProcessing(blocks);
    }

    public CepCnp(WeightingScheme scheme) {
        super(scheme);
        counter = 0;
        nodeCentric = true;
        topComparisons = new HashSet<>();
        this.fixedThreshold = 0;
    }

    public CepCnp(WeightingScheme scheme, int fixedThreshold) {
        super(scheme);
        counter = 0;
        nodeCentric = true;
        topComparisons = new HashSet<>();
        this.fixedThreshold = fixedThreshold;
    }

    private void buildBlocks(List<EntityProfile>[] profiles) {
//        TokenBlocking tb = new TokenBlocking(profiles);
//        blocks = tb.buildBlocks();
//
//        ComparisonsBasedBlockPurging cbbp = new ComparisonsBasedBlockPurging(SMOOTH_FACTOR);
//        cbbp.applyProcessing(blocks);
//
//        BlockFiltering bf = new BlockFiltering(FILTER_RATIO);
//        bf.applyProcessing(blocks);
    }

    @Override
    public boolean hasNext() {
        return counter < sortedTopComparisons.length;
    }

    @Override
    public Comparison next() {
        return sortedTopComparisons[counter++];
    }

    @Override
    protected List<AbstractBlock> pruneEdges() {
        // it does not generate new blocks
        final List<AbstractBlock> newBlocks = new ArrayList<>();

        long t0 = System.currentTimeMillis();

        setLimits();
        //topKEdges = new PriorityQueue<>((int) (2 * threshold), new MetablockingComparator());
        topKEdges = MinMaxPriorityQueue.orderedBy(new InverseMetablockingComparator()).maximumSize((int) (threshold)).create();
        /*topKEdges = MinMaxPriorityQueue.orderedBy(new MetablockingComparator()).create();*/

        long t1 = System.currentTimeMillis();
        System.out.println("pruneEdges - init time: " + (t1-t0) + " ms");

        if (weightingScheme.equals(WeightingScheme.ARCS)) {
            for (int i = firstId; i < lastId; i++) {
                minimumWeight = Float.MIN_VALUE;
                topKEdges.clear();
                processArcsEntity(i);
                verifyValidEntities(i);
            }
        } else {
            for (int i = firstId; i < lastId; i++) {
                minimumWeight = Float.MIN_VALUE;
                topKEdges.clear();
                processEntity(i);
                verifyValidEntities(i);
            }
        }

        long t2 = System.currentTimeMillis();
        System.out.println("pruneEdges - mid time: " + (t2-t1) + " ms");

        // topComparison has no duplicated comparisons
        List<Comparison> sortedComparisons = new ArrayList<>(topComparisons);
        topComparisons.clear();
        // sort
        Collections.sort(sortedComparisons, new InverseMetablockingComparator());

        sortedTopComparisons = sortedComparisons.toArray(new Comparison[sortedComparisons.size()]);
        System.out.println(sortedTopComparisons.length + " comparisons ready!");

        long t3 = System.currentTimeMillis();
        System.out.println("pruneEdges - end time: " + (t3-t2) + " ms");

        return newBlocks;
    }

    protected void setLimits() {
        firstId = 0;
        lastId = noOfEntities;
    }

    @Override
    protected void setThreshold() {
        threshold = this.fixedThreshold != 0 ?
                this.fixedThreshold
                : 10 * Math.max(1, blockAssingments / noOfEntities);
        System.out.println("Threshold: " + threshold);
    }

    @Override
    protected void verifyValidEntities(int entityId) {
        TIntIterator it = validEntities.iterator();
        while (it.hasNext()) {
            int neighborId = it.next();
            Float weight = getWeight(entityId, neighborId);
            if (weight < minimumWeight) {
                continue;
            }

            Comparison comparison = getComparison(entityId, neighborId);
            comparison.setUtilityMeasure(weight);

            topKEdges.offer(comparison);
            // Using MinMaxPriorityQueue from Guava the following is not needed anymore:
            /*topKEdges.add(comparison);
            if (threshold < topKEdges.size()) {
                Comparison lastComparison = topKEdges.poll();
                minimumWeight = lastComparison.getUtilityMeasure();
            }*/
        }
        topComparisons.addAll(topKEdges);
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        refineBlocks(blocks);
        //applyMainProcessing(blocks);
    }

    @Override
    public String getName() {
        return threshold != 0 ? "CepCnpFixed" + threshold : "CepCnp";
    }

    public void setThreshold(int t) {
        this.fixedThreshold = t;
    }

    @Override
    public void developBlockBasedSchedule(List<AbstractBlock> blocks) {
        applyProcessing(blocks);
    }

    @Override
    public void developEntityBasedSchedule(List<EntityProfile> entitiesD1) {

    }

    @Override
    public void developEntityBasedSchedule(List<EntityProfile> entitiesD1, List<EntityProfile> entitiesD2) {

    }
}

