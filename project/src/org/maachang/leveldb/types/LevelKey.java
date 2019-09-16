package org.maachang.leveldb.types;

import org.maachang.leveldb.JniBuffer;

/**
 * Leveldb用複数キー情報.
 */
public interface LevelKey<T> extends Comparable<T> {
	/**
	 * キーサイズを取得.
	 * @return
	 */
	public int size();
	
	/**
	 * キーを取得.
	 * @param no
	 * @return
	 */
	public Object get(int no);
	
	/**
	 * JniBuffer出力.
	 * 
	 * @param buf
	 *            対象のJniBufferを設定します.
	 */
	public void out(JniBuffer buf);
	
	/**
	 * キータイプを取得.
	 * @return
	 */
	public int getType();
}
