/*
 * Copyright (c) 2015 Ramon Servadei 
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractSequentialList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * A version of the standard {@link LinkedList} that uses an internal pool of objects to wrap and
 * hold entries added to the list. This technique reduces garbage churn at the expense of a higher
 * memory footprint and some extra (minor) processing.
 * <p>
 * This is not thread-safe.
 * 
 * @author Ramon Servadei
 */
public class LowGcLinkedList<E> extends AbstractSequentialList<E> implements Deque<E>, Serializable
{
    private static final long serialVersionUID = 1L;

    private final static class Node<E>
    {
        E item;
        Node<E> next;
        Node<E> prev;

        Node()
        {
        }
    }

    transient int size = 0;
    transient Node<E> first;
    transient Node<E> last;

    Node<E> spare;
    
    public LowGcLinkedList()
    {
    }

    public LowGcLinkedList(Collection<? extends E> c)
    {
        this();
        addAll(c);
    }

    private void linkFirst(E e)
    {
        final Node<E> f = this.first;
        final Node<E> newNode = wrap(null, e, f);
        this.first = newNode;
        if (f == null)
        {
            this.last = newNode;
        }
        else
        {
            f.prev = newNode;
        }
        this.size++;
        this.modCount++;
    }

    void linkLast(E e)
    {
        final Node<E> l = this.last;
        final Node<E> newNode = wrap(l, e, null);
        this.last = newNode;
        if (l == null)
        {
            this.first = newNode;
        }
        else
        {
            l.next = newNode;
        }
        this.size++;
        this.modCount++;
    }

    void linkBefore(E e, Node<E> succ)
    {
        final Node<E> pred = succ.prev;
        final Node<E> newNode = wrap(pred, e, succ);
        succ.prev = newNode;
        if (pred == null)
        {
            this.first = newNode;
        }
        else
        {
            pred.next = newNode;
        }
        this.size++;
        this.modCount++;
    }

    private E unlinkFirst(Node<E> f)
    {
        final E element = f.item;
        final Node<E> next = f.next;
        f.item = null;
        release(f);
        f.next = null;
        this.first = next;
        if (next == null)
        {
            this.last = null;
        }
        else
        {
            next.prev = null;
        }
        this.size--;
        this.modCount++;
        return element;
    }

    private E unlinkLast(Node<E> l)
    {
        final E element = l.item;
        final Node<E> prev = l.prev;
        l.item = null;
        release(l);
        l.prev = null;
        this.last = prev;
        if (prev == null)
        {
            this.first = null;
        }
        else
        {
            prev.next = null;
        }
        this.size--;
        this.modCount++;
        return element;
    }

    E unlink(Node<E> x)
    {
        final E element = x.item;
        final Node<E> next = x.next;
        final Node<E> prev = x.prev;

        if (prev == null)
        {
            this.first = next;
        }
        else
        {
            prev.next = next;
            x.prev = null;
        }

        if (next == null)
        {
            this.last = prev;
        }
        else
        {
            next.prev = prev;
            x.next = null;
        }

        x.item = null;
        release(x);
        this.size--;
        this.modCount++;
        return element;
    }

    @Override
    public E getFirst()
    {
        final Node<E> f = this.first;
        if (f == null)
        {
            throw new NoSuchElementException();
        }
        return f.item;
    }

    @Override
    public E getLast()
    {
        final Node<E> l = this.last;
        if (l == null)
        {
            throw new NoSuchElementException();
        }
        return l.item;
    }

    @Override
    public E removeFirst()
    {
        final Node<E> f = this.first;
        if (f == null)
        {
            throw new NoSuchElementException();
        }
        return unlinkFirst(f);
    }

    @Override
    public E removeLast()
    {
        final Node<E> l = this.last;
        if (l == null)
        {
            throw new NoSuchElementException();
        }
        return unlinkLast(l);
    }

    @Override
    public void addFirst(E e)
    {
        linkFirst(e);
    }

    @Override
    public void addLast(E e)
    {
        linkLast(e);
    }

    @Override
    public boolean contains(Object o)
    {
        return indexOf(o) != -1;
    }

    @Override
    public int size()
    {
        return this.size;
    }

    @Override
    public boolean add(E e)
    {
        linkLast(e);
        return true;
    }

    @Override
    public boolean remove(Object o)
    {
        if (o == null)
        {
            for (Node<E> x = this.first; x != null; x = x.next)
            {
                if (x.item == null)
                {
                    unlink(x);
                    return true;
                }
            }
        }
        else
        {
            for (Node<E> x = this.first; x != null; x = x.next)
            {
                if (o.equals(x.item))
                {
                    unlink(x);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends E> c)
    {
        return addAll(this.size, c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c)
    {
        checkPositionIndex(index);

        Object[] a = c.toArray();
        int numNew = a.length;
        if (numNew == 0)
        {
            return false;
        }

        Node<E> pred, succ;
        if (index == this.size)
        {
            succ = null;
            pred = this.last;
        }
        else
        {
            succ = node(index);
            pred = succ.prev;
        }

        for (Object o : a)
        {
            @SuppressWarnings("unchecked")
            E e = (E) o;
            Node<E> newNode = wrap(pred, e, null);
            if (pred == null)
            {
                this.first = newNode;
            }
            else
            {
                pred.next = newNode;
            }
            pred = newNode;
        }

        if (succ == null)
        {
            this.last = pred;
        }
        else
        {
            pred.next = succ;
            succ.prev = pred;
        }

        this.size += numNew;
        this.modCount++;
        return true;
    }

    @Override
    public void clear()
    {
        for (Node<E> x = this.first; x != null;)
        {
            Node<E> next = x.next;
            x.item = null;
            x.next = null;
            x.prev = null;
            x = next;
        }
        this.first = this.last = null;
        this.size = 0;
        this.modCount++;
    }

    @Override
    public E get(int index)
    {
        checkElementIndex(index);
        return node(index).item;
    }

    @Override
    public E set(int index, E element)
    {
        checkElementIndex(index);
        Node<E> x = node(index);
        E oldVal = x.item;
        x.item = element;
        return oldVal;
    }

    @Override
    public void add(int index, E element)
    {
        checkPositionIndex(index);

        if (index == this.size)
        {
            linkLast(element);
        }
        else
        {
            linkBefore(element, node(index));
        }
    }

    @Override
    public E remove(int index)
    {
        checkElementIndex(index);
        return unlink(node(index));
    }

    private boolean isElementIndex(int index)
    {
        return index >= 0 && index < this.size;
    }

    private boolean isPositionIndex(int index)
    {
        return index >= 0 && index <= this.size;
    }

    private String outOfBoundsMsg(int index)
    {
        return "Index: " + index + ", Size: " + this.size;
    }

    private void checkElementIndex(int index)
    {
        if (!isElementIndex(index))
        {
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
        }
    }

    private void checkPositionIndex(int index)
    {
        if (!isPositionIndex(index))
        {
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
        }
    }

    Node<E> node(int index)
    {
        if (index < (this.size >> 1))
        {
            Node<E> x = this.first;
            for (int i = 0; i < index; i++)
            {
                x = x.next;
            }
            return x;
        }
        else
        {
            Node<E> x = this.last;
            for (int i = this.size - 1; i > index; i--)
            {
                x = x.prev;
            }
            return x;
        }
    }

    @Override
    public int indexOf(Object o)
    {
        int index = 0;
        if (o == null)
        {
            for (Node<E> x = this.first; x != null; x = x.next)
            {
                if (x.item == null)
                {
                    return index;
                }
                index++;
            }
        }
        else
        {
            for (Node<E> x = this.first; x != null; x = x.next)
            {
                if (o.equals(x.item))
                {
                    return index;
                }
                index++;
            }
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object o)
    {
        int index = this.size;
        if (o == null)
        {
            for (Node<E> x = this.last; x != null; x = x.prev)
            {
                index--;
                if (x.item == null)
                {
                    return index;
                }
            }
        }
        else
        {
            for (Node<E> x = this.last; x != null; x = x.prev)
            {
                index--;
                if (o.equals(x.item))
                {
                    return index;
                }
            }
        }
        return -1;
    }

    @Override
    public E peek()
    {
        final Node<E> f = this.first;
        return (f == null) ? null : f.item;
    }

    @Override
    public E element()
    {
        return getFirst();
    }

    @Override
    public E poll()
    {
        final Node<E> f = this.first;
        return (f == null) ? null : unlinkFirst(f);
    }

    @Override
    public E remove()
    {
        return removeFirst();
    }

    @Override
    public boolean offer(E e)
    {
        return add(e);
    }

    @Override
    public boolean offerFirst(E e)
    {
        addFirst(e);
        return true;
    }

    @Override
    public boolean offerLast(E e)
    {
        addLast(e);
        return true;
    }

    @Override
    public E peekFirst()
    {
        final Node<E> f = this.first;
        return (f == null) ? null : f.item;
    }

    @Override
    public E peekLast()
    {
        final Node<E> l = this.last;
        return (l == null) ? null : l.item;
    }

    @Override
    public E pollFirst()
    {
        final Node<E> f = this.first;
        return (f == null) ? null : unlinkFirst(f);
    }

    @Override
    public E pollLast()
    {
        final Node<E> l = this.last;
        return (l == null) ? null : unlinkLast(l);
    }

    @Override
    public void push(E e)
    {
        addFirst(e);
    }

    @Override
    public E pop()
    {
        return removeFirst();
    }

    @Override
    public boolean removeFirstOccurrence(Object o)
    {
        return remove(o);
    }

    @Override
    public boolean removeLastOccurrence(Object o)
    {
        if (o == null)
        {
            for (Node<E> x = this.last; x != null; x = x.prev)
            {
                if (x.item == null)
                {
                    unlink(x);
                    return true;
                }
            }
        }
        else
        {
            for (Node<E> x = this.last; x != null; x = x.prev)
            {
                if (o.equals(x.item))
                {
                    unlink(x);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public ListIterator<E> listIterator(int index)
    {
        checkPositionIndex(index);
        return new ListItr(index);
    }

    @SuppressWarnings("synthetic-access")
    private class ListItr implements ListIterator<E>
    {
        private Node<E> lastReturned = null;
        private Node<E> next;
        private int nextIndex;
        private int expectedModCount = LowGcLinkedList.this.modCount;

        ListItr(int index)
        {
            this.next = (index == LowGcLinkedList.this.size) ? null : node(index);
            this.nextIndex = index;
        }

        @Override
        public boolean hasNext()
        {
            return this.nextIndex < LowGcLinkedList.this.size;
        }

        @Override
        public E next()
        {
            checkForComodification();
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }

            this.lastReturned = this.next;
            this.next = this.next.next;
            this.nextIndex++;
            return this.lastReturned.item;
        }

        @Override
        public boolean hasPrevious()
        {
            return this.nextIndex > 0;
        }

        @Override
        public E previous()
        {
            checkForComodification();
            if (!hasPrevious())
            {
                throw new NoSuchElementException();
            }

            this.lastReturned = this.next = (this.next == null) ? LowGcLinkedList.this.last : this.next.prev;
            this.nextIndex--;
            return this.lastReturned.item;
        }

        @Override
        public int nextIndex()
        {
            return this.nextIndex;
        }

        @Override
        public int previousIndex()
        {
            return this.nextIndex - 1;
        }

        @Override
        public void remove()
        {
            checkForComodification();
            if (this.lastReturned == null)
            {
                throw new IllegalStateException();
            }

            Node<E> lastNext = this.lastReturned.next;
            unlink(this.lastReturned);
            if (this.next == this.lastReturned)
            {
                this.next = lastNext;
            }
            else
            {
                this.nextIndex--;
            }
            this.lastReturned = null;
            this.expectedModCount++;
        }

        @Override
        public void set(E e)
        {
            if (this.lastReturned == null)
            {
                throw new IllegalStateException();
            }
            checkForComodification();
            this.lastReturned.item = e;
        }

        @Override
        public void add(E e)
        {
            checkForComodification();
            this.lastReturned = null;
            if (this.next == null)
            {
                linkLast(e);
            }
            else
            {
                linkBefore(e, this.next);
            }
            this.nextIndex++;
            this.expectedModCount++;
        }

        final void checkForComodification()
        {
            if (LowGcLinkedList.this.modCount != this.expectedModCount)
            {
                throw new ConcurrentModificationException();
            }
        }
    }

    private void release(Node<E> node)
    {
        if (this.spare == null)
        {
            this.spare = node;
        }
        else
        {
            node.prev = this.spare;
            this.spare = node;
        }
    }

    private Node<E> wrap(Node<E> prev, E element, Node<E> next)
    {
        Node<E> node = null;
        if (this.spare != null)
        {
            node = this.spare;
            this.spare = this.spare.prev;
        }
        else
        {
            node = new Node<E>();
        }
        node.prev = prev;
        node.next = next;
        node.item = element;
        return node;
    }

    @Override
    public Iterator<E> descendingIterator()
    {
        return new DescendingIterator();
    }

    private class DescendingIterator implements Iterator<E>
    {
        private final ListItr itr = new ListItr(size());

        DescendingIterator()
        {
        }

        @Override
        public boolean hasNext()
        {
            return this.itr.hasPrevious();
        }

        @Override
        public E next()
        {
            return this.itr.previous();
        }

        @Override
        public void remove()
        {
            this.itr.remove();
        }
    }

    @SuppressWarnings("unchecked")
    private LowGcLinkedList<E> superClone()
    {
        try
        {
            return (LowGcLinkedList<E>) super.clone();
        }
        catch (CloneNotSupportedException e)
        {
            throw new InternalError();
        }
    }

    @Override
    public Object clone()
    {
        LowGcLinkedList<E> clone = superClone();

        clone.first = clone.last = null;
        clone.size = 0;
        clone.modCount = 0;

        for (Node<E> x = this.first; x != null; x = x.next)
        {
            clone.add(x.item);
        }

        return clone;
    }

    @Override
    public Object[] toArray()
    {
        Object[] result = new Object[this.size];
        int i = 0;
        for (Node<E> x = this.first; x != null; x = x.next)
        {
            result[i++] = x.item;
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a)
    {
        if (a.length < this.size)
        {
            a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), this.size);
        }
        int i = 0;
        Object[] result = a;
        for (Node<E> x = this.first; x != null; x = x.next)
        {
            result[i++] = x.item;
        }

        if (a.length > this.size)
        {
            a[this.size] = null;
        }

        return a;
    }

    private void writeObject(ObjectOutputStream s) throws IOException
    {
        s.defaultWriteObject();
        s.writeInt(this.size);
        for (Node<E> x = this.first; x != null; x = x.next)
        {
            s.writeObject(x.item);
        }
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException
    {
        s.defaultReadObject();
        int size = s.readInt();
        for (int i = 0; i < size; i++)
        {
            linkLast((E) s.readObject());
        }
    }
}
