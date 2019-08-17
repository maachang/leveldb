package org.maachang.leveldb;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * LeveldbのMap実装.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class LevelMap implements Map<Object, Object> {

	/** LevelDbオブジェクト. **/
	protected Leveldb leveldb;

	/** LevelDbマップセット用オブジェクト. **/
	protected LevelMapSet set;

	/** キータイプ. **/
	protected int type;

	/**
	 * コンストラクタ.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public LevelMap(String name) throws Exception {
		this(name, null);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 */
	public LevelMap(String name, LevelOption option) throws Exception {
		this.leveldb = new Leveldb(name, option);
		this.type = this.leveldb.getOption().getType();
		this.set = null;
	}

	/**
	 * デストラクタ.
	 */
	protected final void finalize() throws Exception {
		close();
	}

	/**
	 * オブジェクトクローズ.
	 */
	public final void close() {
		leveldb.close();
	}

	/**
	 * クローズしているかチェック.
	 * 
	 * @return boolean [true]の場合、クローズしています.
	 */
	public final boolean isClose() {
		return leveldb.isClose();
	}

	/**
	 * Leveldbオブジェクトを取得.
	 * 
	 * @return Leveldb Leveldbオブジェクトが返却されます.
	 */
	public final Leveldb getLeveldb() {
		return leveldb;
	}

	/**
	 * Leveldbのオプションを取得.
	 * 
	 * @return LevelOption オプションが返却されます.
	 */
	public final LevelOption getOption() {
		return leveldb.getOption();
	}

	/**
	 * Leveldbキータイプを取得.
	 * 
	 * @return int キータイプが返却されます.
	 */
	public final int getType() {
		return type;
	}

	/**
	 * 情報クリア. ※Iteratorで処理をするので、件数が多い場合は、処理に時間がかかります.
	 * この処理を呼び出すと、対象のLeveldbに登録されている すべての要素をすべてクリアします.
	 */
	public final void clear() {
		JniBuffer key = null;
		try {

			// Iteratorで削除するので、超遅い.
			LeveldbIterator it = leveldb.iterator();
			key = LevelBuffer.key(type, null);
			while (it.valid()) {
				key.clear(true);
				if (it.key(key) > 0) {
					leveldb.remove(key);
				}
				it.next();
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (key != null) {
				key.clear(true);
			}
		}
	}

	/**
	 * 指定Map情報の内容をすべてセット.
	 * 
	 * @param toMerge
	 *            追加対象のMapを設定します.
	 */
	public final void putAll(Map toMerge) {
		Object k;
		Iterator it = toMerge.keySet().iterator();
		while (it.hasNext()) {
			put((k = it.next()), toMerge.get(k));
		}
	}

	/**
	 * 指定要素が存在するかチェック. ※Iteratorでチェックするので、件数が多い場合は、処理に時間がかかります.
	 * 
	 * @param value
	 *            対象のValueを設定します.
	 * @return boolean trueの場合、一致する条件が存在します.
	 */
	public final boolean containsValue(Object value) {

		// Iteratorで、存在するまでチェック(超遅い).
		JniBuffer v = null;
		try {
			LeveldbIterator it = leveldb.iterator();
			v = LevelBuffer.value();
			if (value == null) {
				while (it.valid()) {
					v.clear();
					if (it.value(v) > 0) {
						if (LevelValues.decode(v) == null) {
							return true;
						}
					}
					it.next();
				}
			} else {
				while (it.valid()) {
					v.clear();
					if (it.value(v) > 0) {
						if (value.equals(LevelValues.decode(v))) {
							return true;
						}
					}
					it.next();
				}
			}
			return false;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (v != null) {
				v.clear();
			}
		}
	}

	/**
	 * この処理はLeveMapでは何もしません. return 例外が返却されます.
	 */
	public Set entrySet() {
		throw new LeveldbException("サポートされていません");
	}

	/**
	 * この処理はLeveMapでは何もしません. return 例外が返却されます.
	 */
	public Collection values() {
		throw new LeveldbException("サポートされていません");
	}

	/**
	 * 指定キーの情報をセット.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param twoKey
	 *            対象のセカンドキーを設定します.
	 * @param value
	 *            対象の要素を設定します. この条件は、数値、文字列、日付系(java.util.Date),配列、
	 *            List、Map、Set、Serializableオブジェクト以外をセットすると、 エラーととなります.
	 * @return Object [null]が返却されます.
	 */
	public final Object put(Object key, Object twoKey, Object value) {
		if (value != null && value instanceof LevelMap) {
			throw new LeveldbException("要素にLevelMap要素は設定できません");
		}
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			keyBuf = LevelBuffer.key(type, key, twoKey);
			if (value instanceof JniBuffer) {
				leveldb.put(keyBuf, (JniBuffer) value);
			} else {
				valBuf = LevelBuffer.value(value);
				leveldb.put(keyBuf, valBuf);
			}
			return null;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (keyBuf != null) {
				keyBuf.clear(true);
			}
			if (valBuf != null) {
				valBuf.clear();
			}
		}
	}

	/**
	 * 指定キーの情報をセット.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param value
	 *            対象の要素を設定します. この条件は、数値、文字列、日付系(java.util.Date),配列、
	 *            List、Map、Set、Serializableオブジェクト以外をセットすると、 エラーととなります.
	 * @return Object [null]が返却されます.
	 */
	public final Object put(Object key, Object value) {
		return put(key, null, value);
	}

	/**
	 * 指定キーの情報をセット.
	 * 
	 * @param value
	 *            対象の要素を設定します. この条件は、数値、文字列、日付系(java.util.Date),配列、
	 *            List、Map、Set、Serializableオブジェクト以外をセットすると、 エラーととなります.
	 * @param key
	 *            対象のキー群を設定します.
	 * @return Object [null]が返却されます.
	 */
	public final Object putMultiKey(Object value, Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb定義のキータイプはマルチキーではありません");
		}
		return put(keys, null, value);
	}

	/**
	 * 指定キー情報が存在するかチェック.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param twoKey
	 *            対象のセカンドキーを設定します.
	 * @return boolean [true]の場合、存在します.
	 */
	public final boolean containsKey(Object key, Object twoKey) {
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			keyBuf = LevelBuffer.key(type, key, twoKey);
			valBuf = LevelBuffer.value();
			int res = leveldb.get(valBuf, keyBuf);
			return res != 0;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (keyBuf != null) {
				keyBuf.clear(true);
			}
			if (valBuf != null) {
				valBuf.clear();
			}
		}
	}

	/**
	 * 指定キー情報が存在するかチェック.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return boolean [true]の場合、存在します.
	 */
	public final boolean containsKey(Object key) {
		return containsKey(key, null);
	}

	/**
	 * 指定キー情報が存在するかチェック.
	 * 
	 * @param keys
	 *            対象のキー群を設定します.
	 * @return boolean [true]の場合、存在します.
	 */
	public final boolean containsMultiKey(Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb定義のキータイプはマルチキーではありません");
		}
		return containsKey(keys, null);
	}

	/**
	 * 指定キー情報に対する要素を取得.
	 * 
	 * @param buf
	 *            対象の要素格納用バッファを設定します.
	 * @param key
	 *            対象のキーを設定します.
	 * @param twoKey
	 *            対象のセカンドキーを設定します.
	 * @return boolean [true]の場合、セットされました.
	 */
	public final boolean getBuffer(JniBuffer buf, Object key, Object twoKey) {
		boolean ret = false;
		JniBuffer keyBuf = null;
		try {
			keyBuf = LevelBuffer.key(type, key, twoKey);
			buf.clear();
			if (leveldb.get(buf, keyBuf) != 0) {
				ret = true;
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (keyBuf != null) {
				keyBuf.clear(true);
			}
			if (!ret) {
				buf.clear();
			}
		}
		return ret;
	}

	/**
	 * 指定キー情報に対する要素を取得.
	 * 
	 * @param buf
	 *            対象の要素格納用バッファを設定します.
	 * @param keys
	 *            対象のキー群を設定します.
	 * @return boolean [true]の場合、セットされました.
	 */
	public final boolean getBufferMultiKey(JniBuffer buf, Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb定義のキータイプはマルチキーではありません");
		}
		return getBuffer(buf, keys, null);
	}

	/**
	 * 指定キー情報に対する要素を取得.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param twoKey
	 *            対象のセカンドキーを設定します.
	 * @return Object 対象の要素が返却されます.
	 */
	public final Object get(Object key, Object twoKey) {
		JniBuffer buf = null;
		try {
			buf = LevelBuffer.value();
			if (getBuffer(buf, key, twoKey)) {
				return LevelValues.decode(buf);
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (buf != null) {
				buf.clear();
			}
		}
		return null;
	}

	/**
	 * 指定キー情報に対する要素を取得.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return Object 対象の要素が返却されます.
	 */
	public final Object get(Object key) {
		return get(key, null);
	}

	/**
	 * 指定キー情報に対する要素を取得.
	 * 
	 * @param keys
	 *            対象のキー群を設定します.
	 * @return Object 対象の要素が返却されます.
	 */
	public final Object getMultiKey(Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb定義のキータイプはマルチキーではありません");
		}
		return get(keys, null);
	}

	/**
	 * 指定キーの情報を削除.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param twoKey
	 *            対象のセカンドキーを設定します.
	 * @return Object 削除できた場合[true]が返却されます.
	 */
	public final boolean remove(Object key, Object twoKey) {
		JniBuffer keyBuf = null;
		try {
			keyBuf = LevelBuffer.key(type, key, twoKey);
			return leveldb.remove(keyBuf);
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (keyBuf != null) {
				keyBuf.clear(true);
			}
		}
	}

	/**
	 * 指定キーの情報を削除.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return Object 削除できた場合[true]が返却されます.
	 */
	public final Object remove(Object key) {
		return remove(key, null);
	}

	/**
	 * 指定キーの情報を削除.
	 * 
	 * @param keys
	 *            対象のキー群を設定します.
	 * @return Object 削除できた場合[true]が返却されます.
	 */
	public final Object removeMultiKey(Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb定義のキータイプはマルチキーではありません");
		}
		return remove(keys, null);
	}

	/**
	 * 情報が空かチェック. return boolean [false]が返却されます.
	 */
	public final boolean isEmpty() {
		try {
			// 1件以上のIteratorが存在する場合は[false].
			LeveldbIterator it = leveldb.iterator();
			if (it.valid()) {
				it.close();
				return false;
			}
			it.close();
			return true;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		}
	}

	/**
	 * Setオブジェクトを取得.
	 * 
	 * @return Set Setオブジェクトが返却されます.
	 */
	public final Set keySet() {
		if (set == null) {
			set = new LevelMapSet(this);
		}
		return set;
	}

	/**
	 * 登録データ数を取得. ※Iteratorでカウントするので、件数が多い場合は、処理に時間がかかります. return int
	 * 登録データ数が返却されます.
	 */
	public final int size() {
		try {
			int ret = 0;

			// Iteratorで削除するので、超遅い.
			LeveldbIterator it = leveldb.iterator();
			while (it.valid()) {
				ret++;
				it.next();
			}
			it.close();
			return ret;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		}
	}

	/**
	 * この処理はLeveMapでは何もしません.
	 * 
	 * @param set
	 *            例外が発生します.
	 */
	public final void getAllKey(Set set) {
		throw new LeveldbException("サポートされていません");
	}

	/**
	 * この処理はLeveMapでは何もしません.
	 * 
	 * @param set
	 *            例外が発生します.
	 */
	public final void getAllValues(Set set) {
		throw new LeveldbException("サポートされていません");
	}

	/**
	 * この処理はLeveMapでは何もしません.
	 * 
	 * @return String 空文字が返却されます.
	 */
	public final String toString() {

		// 何もしない.
		return "";
	}

	/**
	 * 現在オープン中のLeveldbパス名を取得.
	 * 
	 * @return String Leveldbパス名が返却されます.
	 */
	public final String getPath() {
		return leveldb.getPath();
	}

	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public final LevelMapIterator iterator() {
		return _iterator();
	}

	/**
	 * snapshotを取得.
	 * 
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public final LevelMapIterator snapshot() {
		return _snapShot();
	}

	/** iterator作成. **/
	protected final LevelMapIterator _iterator() {
		return new LevelMapIterator(this, type, leveldb.iterator());
	}

	/** snapShort用のIteratorを作成. **/
	protected final LevelMapIterator _snapShot() {
		return new LevelMapIterator(this, type, leveldb.snapShot());
	}

	/** LevelMapSet. **/
	protected static final class LevelMapSet implements Set {
		private final LevelMap map;

		public LevelMapSet(LevelMap map) {
			this.map = map;
		}

		public final boolean add(Object arg0) {
			map.put(arg0, null);
			return false;
		}

		public final boolean addAll(Collection arg0) {
			Iterator it = arg0.iterator();
			while (it.hasNext()) {
				add(it.next());
			}
			return true;
		}

		public final void clear() {
			map.clear();
		}

		public final boolean contains(Object arg0) {
			return map.containsKey(arg0);
		}

		public final boolean containsAll(Collection arg0) {
			Iterator it = arg0.iterator();
			while (it.hasNext()) {
				if (map.containsKey(it.next())) {
					continue;
				}
				return false;
			}
			return true;
		}

		public final boolean isEmpty() {
			return map.isEmpty();
		}

		public final Iterator<Object> iterator() {
			return map._iterator();
		}

		public final boolean remove(Object arg0) {
			return (Boolean) map.remove(arg0);
		}

		public final boolean removeAll(Collection arg0) {
			boolean ret = false;
			Iterator it = arg0.iterator();
			while (it.hasNext()) {
				if ((Boolean) map.remove(it.next())) {
					ret = true;
				}
			}
			return ret;
		}

		public final boolean retainAll(Collection arg0) {
			throw new LeveldbException("サポートされていません");
		}

		public final int size() {
			return map.size();
		}

		public final Object[] toArray() {
			throw new LeveldbException("サポートされていません");
		}

		public final Object[] toArray(Object[] arg0) {
			throw new LeveldbException("サポートされていません");
		}
	}

}
