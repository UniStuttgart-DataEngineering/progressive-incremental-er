package com.parER.progressive.main;



import gnu.trove.iterator.TIntIterator;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.BilateralBlock;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.UnilateralBlock;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

import java.util.List;

/**
 * Created by gio
 * on 24/01/17.
 */
public class EntityFiltering extends CepCnpEntities {

    private Float[] weights;

    public EntityFiltering(WeightingScheme ws, List<AbstractBlock> blocks, int num_profiles) {
        super(ws, num_profiles);
        weights = new Float[num_profiles];

        for (AbstractBlock block_ : blocks) {
            if (block_ instanceof BilateralBlock) {
                BilateralBlock block = (BilateralBlock) block_;
            } else {
                UnilateralBlock block = (UnilateralBlock) block_;
                for (int profile : block.getEntities()) {
                    weights[profile] += (1 /block.getNoOfComparisons());
                }
            }
        }

        for (int i = 0; i < num_profiles; i++) {
            sortedEntityHeap.offer(new EntityComparable(i, weights[i]));
        }
    }

    @Override
    protected void verifyValidEntities(int entityId) {
        Float max_neighbor = Float.MIN_VALUE;
        Comparison max_comparison = null;
        TIntIterator it = validEntities.iterator();
        while (it.hasNext()) {
            int neighborId = it.next();
            Float weight = getWeight(entityId, neighborId);
            if (weight < 0) {
                continue;
            }

            Comparison comparison = getComparison(entityId, neighborId);
            comparison.setUtilityMeasure(weight);

            if (weight > max_neighbor) {
                max_neighbor = weight;
                max_comparison = comparison;
            }
        }

        if (max_comparison != null) {
            topComparisons.add(max_comparison);
        }
    }

    /*@Override
    public void applyProcessing(List<AbstractBlock> blocks) {

    }
    @Override
    public String getName() {
        return null;
    }
    @Override
    public boolean hasNext() {
        return false;
    }
    @Override
    public Comparison next() {
        return null;
    }*/

    @Override
    public String getName() {
        return "entityBF";
    }
}








/*
package BlockBuilding.Progressive.SortedEntities;

        import BlockBuilding.Progressive.DataStructures.EntityComparable;
        import BlockBuilding.Progressive.DataStructures.InverseMetablockingComparator;
        import BlockBuilding.Progressive.ProgressiveMetaBlocking.AbstractProgressiveMetaBlocking;
        import DataStructures.AbstractBlock;
        import DataStructures.BilateralBlock;
        import DataStructures.Comparison;
        import DataStructures.UnilateralBlock;
        import com.google.common.collect.MinMaxPriorityQueue;

        import java.util.*;

*//**
 * Created by gio
 * on 24/01/17.
 *//*
public class EntityFiltering implements Iterator<Comparison>, AbstractProgressiveMetaBlocking {

    private PriorityQueue<EntityComparable> sortedEntityHeap;
    private Float[] weights;
    private boolean[] entityCompared;

    public EntityFiltering(List<AbstractBlock> blocks, int num_profiles) {
        Collections.reverse(blocks);
        sortedEntityHeap = new PriorityQueue<>(num_profiles, Comparator.reverseOrder());
        weights = new Float[num_profiles];
        entityCompared = new boolean[num_profiles];


        for (AbstractBlock block_ : blocks) {
            if (block_ instanceof BilateralBlock) {
                BilateralBlock block = (BilateralBlock) block_;
            } else {
                UnilateralBlock block = (UnilateralBlock) block_;
                for (int profile : block.getEntities()) {
                    weights[profile] += (1 / block.getAggregateCardinality());
                }
            }
        }

        for (int i = 0; i < num_profiles; i++) {
            sortedEntityHeap.offer(new EntityComparable(i, weights[i]));
        }
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {

    }
    @Override
    public String getName() {
        return null;
    }
    @Override
    public boolean hasNext() {
        boolean comparison_not_empty = !entityComparison.isEmpty();

        if (comparison_not_empty) {
            return true;
        }

        int current_id;

        while (!sortedEntityHeap.isEmpty()) {
            current_id = sortedEntityHeap.poll().getId();
            entityCompared[current_id] = true;
            getComparisonEntity(current_id);
            if (!entityComparison.isEmpty()) {
                *//*if (counter == sortedTopComparisons.length) {
                    entityComparison.poll(); // the first has already been compared
                }*//*
                return true;
            }
        }

        return !entityComparison.isEmpty();
    }
    @Override
    public Comparison next() {
        return null;
    }


}*/
