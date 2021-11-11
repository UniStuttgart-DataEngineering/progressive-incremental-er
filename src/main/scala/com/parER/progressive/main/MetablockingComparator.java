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



import org.scify.jedai.datamodel.Comparison;

import java.util.Comparator;

/**
 * @author G.A.P. II
 */

public class MetablockingComparator implements Comparator<Comparison> {

    @Override
    public int compare(Comparison o1, Comparison o2) {
        Float test = o2.getUtilityMeasure() - o1.getUtilityMeasure();
        if (0 < test) {
            return -1;
        }

        if (test < 0) {
            return 1;
        }

        return 0;
    }

}