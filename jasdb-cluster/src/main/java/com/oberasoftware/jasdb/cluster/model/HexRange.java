/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.cluster.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;

/**
 * User: renarj
 * Date: 3/16/12
 * Time: 11:16 AM
 */
public class HexRange {
    public static final char[] HEX_RANGE = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public static HexRange DEFAULT = new HexRange("0", "F");

    private String start;
    private String end;

    public HexRange(String start, String end) {
        this.start = start;
        this.end = end;
    }

    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }

    public List<HexRange> splitHexRange() {
        List<HexRange> ranges = new ArrayList<>();

        String rangeBase = "";
        int rangeLength = start.length();
        if(rangeLength > 1) {
            rangeBase = start.substring(0, rangeLength - 1);
        }

        char lastStartChar = start.charAt(start.length() - 1);
        char lastEndChar = end.charAt(end.length() - 1);
        if(lastEndChar != lastStartChar) {
            int startHexIndex = Arrays.binarySearch(HEX_RANGE, lastStartChar);
            int endHexIndex = Arrays.binarySearch(HEX_RANGE, lastEndChar);

            int dist = (endHexIndex - startHexIndex) + 1;
            int mid = (dist / 2) + startHexIndex;

            ranges.add(new HexRange(rangeBase + lastStartChar, rangeBase + HEX_RANGE[mid - 1]));
            ranges.add(new HexRange(rangeBase + HEX_RANGE[mid], rangeBase + lastEndChar));
        } else {
            rangeBase = rangeBase + lastStartChar;
            ranges.add(new HexRange(rangeBase + HEX_RANGE[0], rangeBase + HEX_RANGE[7]));
            ranges.add(new HexRange(rangeBase + HEX_RANGE[8], rangeBase + HEX_RANGE[15]));
        }

        return ranges;
    }

    public static List<HexRange> generate(int slices) {
        if((slices & -slices) == slices) {
            List<HexRange> ranges = newArrayList(DEFAULT);
            while(ranges.size() < slices) {
                ranges = ranges.stream()
                        .map(HexRange::splitHexRange)
                        .flatMap(Collection::stream)
                        .collect(toList());
            }
            return ranges;
        } else {
            throw new IllegalArgumentException("Slices need to be power of 2");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HexRange hexRange = (HexRange) o;

        if (!start.equals(hexRange.start)) return false;
        return end.equals(hexRange.end);
    }

    @Override
    public int hashCode() {
        int result = start.hashCode();
        result = 31 * result + end.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "HexRange{" +
                "start='" + start + '\'' +
                ", end='" + end + '\'' +
                '}';
    }
}
