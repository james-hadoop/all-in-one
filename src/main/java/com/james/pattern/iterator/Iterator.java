package com.james.pattern.iterator;

public interface Iterator {
    Object next();

    boolean hasNext();

    boolean remove();
}
