package de.upb.wdqa.wsdmcup17.dataserver.util;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Queue;

public class SynchronizedBoundedBlockingMapQueue<K, V> {
	
	private Queue<Entry<K, V>> queue;	
	private Map<K,V> map;
	
	private int capacity;
	
	
	public SynchronizedBoundedBlockingMapQueue(int capacity){
		queue = new LinkedList<>();
		map = new HashMap<K,V>();
		
		this.capacity = capacity; 
	}
	
    /**
     * Inserts the specified element into this queue, waiting if necessary
     * for space to become available.
     * 
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old value is replaced.
     *
     * @param e the element to add
     * @throws InterruptedException if interrupted while waiting
     * @throws ClassCastException if the class of the specified element
     *         prevents it from being added to this queue
     * @throws NullPointerException if the specified element is null
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     */
	public synchronized void put (K key, V value) throws InterruptedException{
		
		// wait until there is sufficient space in the queue
		while(queue.size() >= capacity){
			wait();
		}
		
		if (map.containsKey(key)){
			throw new IllegalArgumentException("Key already present in queue: " + key);
		}
		
		queue.add(new AbstractMap.SimpleEntry<K, V>(key, value));
		
		try{
			map.put(key, value);
		}
		catch(ClassCastException|NullPointerException|IllegalArgumentException e){
			queue.remove(value);
			throw e;
		}
		
		notifyAll(); // now the get operation might return the value
	}
	
    /**
     * Returns the value to which the specified key is mapped,
     * or blocks until this map contains a mapping for the key.
     *
     */
	public synchronized V get(K key) throws InterruptedException{
		
		// wait until the queue contains the key
		while(!map.containsKey(key)){
			wait();
		}
		
		return map.get(key);
	}
	
	
    /**
     * Retrieves, but does not remove, the head of this queue,
     * or returns <tt>null</tt> if this queue is empty.
     *
     * @return the head of this queue, or <tt>null</tt> if this queue is empty
     */
	public synchronized V peek(){
		Entry<K, V> entry = queue.peek();
		
		if (entry == null){
			return null;
		}
		else{
			return entry.getValue();
		}
	}
	
	
    /**
     * Retrieves and removes the head of this queue.  This method differs
     * from {@link #poll poll} only in that it throws an exception if this
     * queue is empty.
     *
     * @return the head of this queue
     * @throws NoSuchElementException if this queue is empty
     */
	public synchronized void remove(){
		Entry<K, V> entry = queue.peek();
		
		if (entry != null){
			if (!map.remove(entry.getKey(), entry.getValue())){
				throw new IllegalStateException("The mapping of keys and values got mixed up");
			}
			
			// no exception has been thrown
			queue.remove();			
		}
		
		notifyAll(); // now it might be possible to put new elements in the queue
	}
	
	
    /**
     * Returns the number of elements in this collection.  If this collection
     * contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of elements in this collection
     */
	public synchronized int  size(){
		int result = queue.size();
		
		if (result != map.size()){
			throw new IllegalStateException("The queue and map have a different size");
		}
		
		return result;
	}
	
}