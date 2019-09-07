package org.maachang.leveldb;

import java.util.Iterator;

/**
 * LevelIterator.
 */
public interface LevelIterator<E> extends Iterator<E> {
	/**
	 * クローズ処理.
	 */
	public void close();
	
	/**
	 * 逆カーソル移動かチェック.
	 * @return
	 */
	public boolean isReverse();
}
