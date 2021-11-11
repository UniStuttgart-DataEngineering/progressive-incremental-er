package com.parER.progressive.main;


import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.Comparison;

import java.util.Iterator;
import java.util.List;

/**
 * @author giovanni
 */
public interface AbstractProgressiveMetaBlocking extends Iterator<Comparison> {
    void applyProcessing(List<AbstractBlock> blocks);
    String getName();
    //boolean hasNext();
    //Comparison next();
}
