/*
 * Copyright 2020 Ramon Servadei
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fimtra.util;

import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Synchronized wrapper for a {@link Deque}
 *
 * @author Ramon Servadei
 */
public final class SynchronizedDeque<T> implements Deque<T> {

    private final Deque<T> delegate;

    public SynchronizedDeque(Deque<T> delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public synchronized void addFirst(T t)
    {
        this.delegate.addFirst(t);
    }

    @Override
    public synchronized void addLast(T t)
    {
        this.delegate.addLast(t);
    }

    @Override
    public synchronized boolean offerFirst(T t)
    {
        return this.delegate.offerFirst(t);
    }

    @Override
    public synchronized boolean offerLast(T t)
    {
        return this.delegate.offerLast(t);
    }

    @Override
    public synchronized T removeFirst()
    {
        return this.delegate.removeFirst();
    }

    @Override
    public synchronized T removeLast()
    {
        return this.delegate.removeLast();
    }

    @Override
    public synchronized T pollFirst()
    {
        return this.delegate.pollFirst();
    }

    @Override
    public synchronized T pollLast()
    {
        return this.delegate.pollLast();
    }

    @Override
    public synchronized T getFirst()
    {
        return this.delegate.getFirst();
    }

    @Override
    public synchronized T getLast()
    {
        return this.delegate.getLast();
    }

    @Override
    public synchronized T peekFirst()
    {
        return this.delegate.peekFirst();
    }

    @Override
    public synchronized T peekLast()
    {
        return this.delegate.peekLast();
    }

    @Override
    public synchronized boolean removeFirstOccurrence(Object o)
    {
        return this.delegate.removeFirstOccurrence(o);
    }

    @Override
    public synchronized boolean removeLastOccurrence(Object o)
    {
        return this.delegate.removeLastOccurrence(o);
    }

    @Override
    public synchronized boolean add(T t)
    {
        return this.delegate.add(t);
    }

    @Override
    public synchronized boolean offer(T t)
    {
        return this.delegate.offer(t);
    }

    @Override
    public synchronized T remove()
    {
        return this.delegate.remove();
    }

    @Override
    public synchronized T poll()
    {
        return this.delegate.poll();
    }

    @Override
    public synchronized T element()
    {
        return this.delegate.element();
    }

    @Override
    public synchronized T peek()
    {
        return this.delegate.peek();
    }

    @Override
    public synchronized void push(T t)
    {
        this.delegate.push(t);
    }

    @Override
    public synchronized T pop()
    {
        return this.delegate.pop();
    }

    @Override
    public synchronized boolean remove(Object o)
    {
        return this.delegate.remove(o);
    }

    @Override
    public synchronized boolean contains(Object o)
    {
        return this.delegate.contains(o);
    }

    @Override
    public synchronized int size()
    {
        return this.delegate.size();
    }

    @Override
    public synchronized Iterator<T> iterator()
    {
        return this.delegate.iterator();
    }

    @Override
    public synchronized Iterator<T> descendingIterator()
    {
        return this.delegate.descendingIterator();
    }

    @Override
    public synchronized boolean isEmpty()
    {
        return this.delegate.isEmpty();
    }

    @Override
    public synchronized Object[] toArray()
    {
        return this.delegate.toArray();
    }

    @Override
    public synchronized <T1> T1[] toArray(T1[] a)
    {
        return this.delegate.toArray(a);
    }

    @Override
    public synchronized boolean containsAll(Collection<?> c)
    {
        return this.delegate.containsAll(c);
    }

    @Override
    public synchronized boolean addAll(Collection<? extends T> c)
    {
        return this.delegate.addAll(c);
    }

    @Override
    public synchronized boolean removeAll(Collection<?> c)
    {
        return this.delegate.removeAll(c);
    }

    @Override
    public synchronized boolean removeIf(Predicate<? super T> filter)
    {
        return this.delegate.removeIf(filter);
    }

    @Override
    public synchronized boolean retainAll(Collection<?> c)
    {
        return this.delegate.retainAll(c);
    }

    @Override
    public synchronized void clear()
    {
        this.delegate.clear();
    }

    @Override
    public synchronized boolean equals(Object o)
    {
        return this.delegate.equals(o);
    }

    @Override
    public synchronized int hashCode()
    {
        return this.delegate.hashCode();
    }

    @Override
    public synchronized Spliterator<T> spliterator()
    {
        return this.delegate.spliterator();
    }

    @Override
    public synchronized Stream<T> stream()
    {
        return this.delegate.stream();
    }

    @Override
    public synchronized Stream<T> parallelStream()
    {
        return this.delegate.parallelStream();
    }

    @Override
    public synchronized void forEach(Consumer<? super T> action)
    {
        this.delegate.forEach(action);
    }
}
