package org.maachang.leveldb;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * LevelMap用Iterator.
 */
@SuppressWarnings({"rawtypes"})
public class LevelMapIterator implements Iterator<Object> {
	private Map map;
	private LeveldbIterator itr;
	private Object nowKey;
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
				itr.seek((keyBuf = LevelBuffer.key(type, key, key2)));
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (keyBuf != null) {
				keyBuf.clear(true);
			}
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
			nowKey = null;
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
			nowKey = null;
			throw new NoSuchElementException("終端まで読まれました");
		}
		_next();
		return nowKey;
	}

	/** 情報を取得. **/
	private void _next() {
		if (itr == null || !itr.valid()) {
			throw new NoSuchElementException("終端まで読まれました");
		}
		JniBuffer buf = null;
		JniBuffer valBuf = null;
		try {
			buf = LevelBuffer.key(type, null);
			valBuf = LevelBuffer.value();
			if (itr.key(buf) <= 0) {
				nowKey = null;
			}
			nowKey = LevelId.get(type, buf);
			itr.next();
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (buf != null) {
				buf.clear(true);
			}
			if (valBuf != null) {
				valBuf.clear();
			}
		}
	}

	/**
	 * 対象情報を削除します.
	 */
	public void remove() {
		if (nowKey != null) {
			map.remove(nowKey);
		} else {
			throw new IllegalStateException(
					"データは存在しないか、削除済みか、next()処理が行われていません");
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
