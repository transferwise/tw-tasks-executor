package com.transferwise.tasks.utils;

@FunctionalInterface
public interface TriConsumer<A, B, C> {

  boolean accept(A a, B b, C c);
}
