package org.maachang.leveldb;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * LevelMap用Iterator.
 */
@SuppressWarnings({ "rawtypes" })
public class LevelMapIterator implements Iterator<Object> {
	private Map map;
	private LeveldbIterator itr;
	private int type;

	/**
	 * LevelMapIteratorの作成.
	 * 
	 * @param map
	 *            対象の親オブジェクトを設定します.
	 * @param type
	 *            対象のキータイプを設定します.
	 * @param itr
	 *            LeveldbIteratorオブジェクトを設定します.
	 */
	protected LevelMapIterator(Map map, int type, LeveldbIterator itr) {
		this.map = map;
		this.itr = itr;
		this.type = type;
	}

	/**
	 * クローズ処理.
	 */
	public void close() {
		if (itr != null) {
			itr.close();
			itr = null;
			map = null;
		}
	}

	/**
	 * 指定内容を検索.
	 * 
	 * @param key
	 *            対象の内容を検索します.
	 * @return LevelMapIterator オブジェクトが返却されます.
	 */
	public LevelMapIterator search(Object key) {
		return search(key, null);
	}

	/**
	 * 指定内容を検索.
	 * 
	 * @param key
	 *            対象の内容を検索します.
	 * @return LevelMapIterator オブジェクトが返却されます.
	 */
	public LevelMapIterator search(Object key, Object key2) {
		if (itr == null) {
			return this;
		}
		JniBuffer keyBuf = null;
		try {
			if (key instanceof JniBuffer) {
				itr.seek((JniBuffer) key);
			} else {
				keyBuf = LevelBuffer.key(type, key, key2);
				itr.seek(keyBuf);
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, null);
		}
		return this;
	}

	/**
	 * 最初のカーソルに移動.
	 */
	public void first() {
		if (itr == null) {
			return;
		}
		itr.first();
	}

	/**
	 * 最後のカーソルに移動.
	 */
	public void last() {
		if (itr == null) {
			return;
		}
		itr.last();
	}

	/**
	 * 次の情報が存在するかチェック.
	 * 
	 * @return boolean [true]の場合、存在します.
	 */
	public boolean hasNext() {
		if (itr == null || !itr.valid()) {
			close();
			return false;
		}
		return true;
	}

	/**
	 * 次の要素を取得.
	 * 
	 * @return Object 次の要素が返却されます.
	 */
	public Object next() {
		if (itr == null || !itr.valid()) {
			close();
			throw new NoSuchElementException();
		}
		JniBuffer keyBuf = null;
		try {
			keyBuf = LevelBuffer.key();
			itr.key(keyBuf);
			Object ret = LevelId.get(type, keyBuf);
			LevelBuffer.clearBuffer(keyBuf, null);
			keyBuf = null;
			itr.next();
			if(!itr.valid()) {
				close();
			}
			return ret;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, null);
		}
	}

	/**
	 * 対象情報を削除します.
	 */
	public void remove() {
		if (itr == null || !itr.valid()) {
			close();
			throw new IllegalStateException(
				"Data does not exist, has been deleted, or next() "
				+ "processing has not been performed.");
		}
		JniBuffer keyBuf = null;
		try {
			keyBuf = LevelBuffer.key();
			itr.key(keyBuf);
			map.remove(LevelId.get(type, keyBuf));
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, null);
		}
	}

	/**
	 * 対象タイプを取得.
	 * 
	 * @return int 対象タイプが返却されます.
	 */
	public int getType() {
		return type;
	}
}
