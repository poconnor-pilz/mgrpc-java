package com.pilz.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ToList {

    //Taken from apache commons IteratorUtils
    public static <E> List<E> toList(final Iterator<? extends E> iterator) {
        if (iterator == null) {
            throw new NullPointerException("Iterator must not be null");
        }
        final List<E> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }
}
