package org.maachang.leveldb.operator;

import java.util.Iterator;

/**
 * LevelIterator.
 */
public abstract class LevelIterator<K,V> implements Iterator<V> {
	protected boolean reverse = false;
	protected Object resultKey = null;
	
	/**
	 * クローズ処理.
	 */
	public abstract void close();
	
	/**
	 * クローズ済みかチェック.
	 * @return
	 */
	public abstract boolean isClose();
	
	/**
	 * 逆カーソル移動かチェック.
	 * @return
	 */
	public boolean isReverse() {
		return reverse;
	}
	
	/**
	 * キー名を取得.
	 * @return Object キー名が返却されます.
	 */
	@SuppressWarnings("unchecked")
	public K getKey() {
		return (K)resultKey;
	}
}
