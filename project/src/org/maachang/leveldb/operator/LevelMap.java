package org.maachang.leveldb.operator;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.JniIO;
import org.maachang.leveldb.LevelBuffer;
import org.maachang.leveldb.LevelId;
import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.LevelValues;
import org.maachang.leveldb.Leveldb;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.LeveldbIterator;
import org.maachang.leveldb.WriteBatch;
import org.maachang.leveldb.util.ConvertMap;

/**
 * LeveldbのMap実装.
 */
@SuppressWarnings("rawtypes")
public class LevelMap extends LevelIndexOperator implements ConvertMap {
	protected LevelMapSet set;
	protected int type;
	
	/**
	 * オペレータタイプ.
	 * @return int オペレータタイプが返却されます.
	 */
	@Override
	public int getOperatorType() {
		return LEVEL_MAP;
	}

	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 */
	public LevelMap(String name) {
		this(name, null);
	}

	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 */
	public LevelMap(String name, LevelOption option) {
		Leveldb db = new Leveldb(name, option);
		super.init(null, db, true, false);
		this.type = db.getOption().getType();
		this.set = null;
		
		// インデックス初期化処理.
		super.initIndex(null);
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを有効にして生成します.
	 * 
	 * @param src 親となるオペレータを設定します.
	 */
	public LevelMap(LevelMap src) {
		// leveldbをクローズせずwriteBatchで処理する.
		super.init(src, src.leveldb, false, true);
		this.type = src.getOption().getType();
		this.set = null;
		
		// インデックス初期化処理.
		super.initIndex(src);
	}

	@Override
	public void close() {
		this.set = null;
		super.close();
	}
	
	/**
	 * 情報のクリア.
	 * LevelMapはこの処理はサポートされていません.
	 */
	public void clear() {
		throw new LeveldbException("clear is not support.");
	}

	/**
	 * 指定Map情報の内容をすべてセット.
	 * 
	 * @param toMerge
	 *            追加対象のMapを設定します.
	 */
	public void putAll(Map toMerge) {
		checkClose();
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
	public boolean containsValue(Object value) {
		checkClose();
		// Iteratorで、存在するまでチェック(超遅い).
		JniBuffer valBuf = null;
		LeveldbIterator it = null;
		try {
			if(writeBatchFlag) {
				it = getSnapshot();
				it.first();
			} else {
				it = leveldb.iterator();
			}
			valBuf = LevelBuffer.value();
			if (value == null) {
				while (it.valid()) {
					LevelBuffer.clearBuffer(null, valBuf);
					if (it.value(valBuf) > 0 && LevelValues.decode(valBuf) == null) {
						return true;
					}
					it.next();
				}
			} else {
				while (it.valid()) {
					LevelBuffer.clearBuffer(null, valBuf);
					if (it.value(valBuf) > 0 && value.equals(LevelValues.decode(valBuf))) {
						return true;
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
			if(!writeBatchFlag) {
				it.close();
			}
			LevelBuffer.clearBuffer(null, valBuf);
		}
	}

	/**
	 * この処理はLeveMapでは何もしません. return 例外が返却されます.
	 */
	public Set entrySet() {
		throw new LeveldbException("Not supported.");
	}

	/**
	 * この処理はLeveMapでは何もしません. return 例外が返却されます.
	 */
	public Collection values() {
		throw new LeveldbException("Not supported.");
	}
	
	// キー用のJniBufferを取得.
	private final JniBuffer _getKey(Object key, Object twoKey)
		throws Exception {
		return _getKey(true, key, twoKey);
	}
	
	// キー用のJniBufferを取得.
	private final JniBuffer _getKey(boolean jniBufFlg, Object key, Object twoKey)
		throws Exception {
		// インデックスの操作が必要な場合は、jniBufFlg は false.
		if (key instanceof JniBuffer) {
			if(!jniBufFlg) {
				throw new LeveldbException("JniBuffer cannot be set for key.");
			}
			return (JniBuffer) key;
		}
		return LevelBuffer.key(type, key, twoKey);
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
	public Object put(Object key, Object twoKey, Object value) {
		checkClose();
		if (value != null && value instanceof LevelOperator) {
			throw new LeveldbException("LevelOperator element cannot be set for the element.");
		}
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			keyBuf = _getKey(false, key, twoKey);
			if (value instanceof JniBuffer) {
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, (JniBuffer) value);
				} else {
					leveldb.put(keyBuf, (JniBuffer) value);
				}
				// インデックス処理.
				super.putIndex(key, twoKey, LevelValues.decode((JniBuffer) value));
			} else {
				valBuf = LevelBuffer.value(value);
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, valBuf);
				} else {
					leveldb.put(keyBuf, valBuf);
				}
				// インデックス処理.
				super.putIndex(key, twoKey, value);
			}
			return null;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, valBuf);
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
	public Object put(Object key, Object value) {
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
	public Object putMultiKey(Object value, Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb definition key type is not multi-key.");
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
	public boolean containsKey(Object key, Object twoKey) {
		checkClose();
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			keyBuf = _getKey(key, twoKey);
			if(writeBatchFlag) {
				LeveldbIterator snapshot = getSnapshot();
				snapshot.seek(keyBuf);
				if (snapshot.valid()) {
					valBuf = LevelBuffer.value();
					snapshot.key(valBuf);
					return JniIO.equals(keyBuf.address(), keyBuf.position(), valBuf.address(), valBuf.position());
				}
				return false;
			} else {
				valBuf = LevelBuffer.value();
				int res = leveldb.get(valBuf, keyBuf);
				return res != 0;
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, valBuf);
		}
	}

	/**
	 * 指定キー情報が存在するかチェック.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return boolean [true]の場合、存在します.
	 */
	public boolean containsKey(Object key) {
		return containsKey(key, null);
	}

	/**
	 * 指定キー情報が存在するかチェック.
	 * 
	 * @param keys
	 *            対象のキー群を設定します.
	 * @return boolean [true]の場合、存在します.
	 */
	public boolean containsMultiKey(Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb definition key type is not multi-key.");
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
	public boolean getBuffer(JniBuffer buf, Object key, Object twoKey) {
		checkClose();
		boolean ret = false;
		JniBuffer keyBuf = null;
		try {
			keyBuf = _getKey(key, twoKey);
			if(writeBatchFlag) {
				LeveldbIterator snapshot = getSnapshot();
				snapshot.seek(keyBuf);
				// 条件が存在する場合.
				if (snapshot.valid()) {
					snapshot.key(buf);
					// 対象キーが正しい場合.
					if (JniIO.equals(keyBuf.address(), keyBuf.position(), buf.address(), buf.position())) {
						snapshot.value(buf);
						ret = true;
					}
				}
			}
			if (leveldb.get(buf, keyBuf) != 0) {
				ret = true;
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (!(key instanceof JniBuffer)) {
				LevelBuffer.clearBuffer(keyBuf, null);
			}
		}
		return ret;
	}

	/**
	 * 指定キー情報に対する要素を取得.
	 * 
	 * @param buf
	 *            対象の要素格納用バッファを設定します.
	 * @param key
	 *            対象のキーを設定します.
	 * @return boolean [true]の場合、セットされました.
	 */
	public boolean getBuffer(JniBuffer buf, Object key) {
		return getBuffer(buf, key, null);
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
	public boolean getBufferMultiKey(JniBuffer buf, Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb definition key type is not multi-key.");
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
	public Object get(Object key, Object twoKey) {
		checkClose();
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
			LevelBuffer.clearBuffer(null, buf);
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
	public Object get(Object key) {
		return get(key, null);
	}

	/**
	 * 指定キー情報に対する要素を取得.
	 * 
	 * @param keys
	 *            対象のキー群を設定します.
	 * @return Object 対象の要素が返却されます.
	 */
	public Object getMultiKey(Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb definition key type is not multi-key.");
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
	public boolean remove(Object key, Object twoKey) {
		checkClose();
		JniBuffer keyBuf = null;
		try {
			Object v = get(key, twoKey);
			keyBuf = _getKey(false, key, twoKey);
			if(writeBatchFlag) {
				WriteBatch b = writeBatch();
				b.remove(keyBuf);
				super.removeIndex(key, twoKey, v);
				return true;
			}
			final boolean ret = leveldb.remove(keyBuf);
			if(ret) {
				super.removeIndex(key, twoKey, v);
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
	 * 指定キーの情報を削除.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return Object 削除できた場合[true]が返却されます.
	 */
	public Object remove(Object key) {
		return remove(key, null);
	}

	/**
	 * 指定キーの情報を削除.
	 * 
	 * @param keys
	 *            対象のキー群を設定します.
	 * @return Object 削除できた場合[true]が返却されます.
	 */
	public Object removeMultiKey(Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb definition key type is not multi-key.");
		}
		return remove(keys, null);
	}

	/**
	 * 情報が空かチェック.
	 * 
	 * @return boolean データが空の場合[true]が返却されます.
	 */
	public boolean isEmpty() {
		checkClose();
		if(writeBatchFlag) {
			try {
				LeveldbIterator snapshot = getSnapshot();
				snapshot.first();
				if (snapshot.valid()) {
					return false;
				}
				return true;
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			}
		}
		return leveldb.isEmpty();
	}

	/**
	 * Setオブジェクトを取得.
	 * 
	 * @return Set Setオブジェクトが返却されます.
	 */
	public Set keySet() {
		checkClose();
		if (set == null) {
			set = new LevelMapSet(this);
		}
		return set;
	}

	/**
	 * 登録データ数を取得.
	 * ※Iteratorでカウントするので、件数が多い場合は、処理に時間がかかります.
	 * @return int 登録データ数が返却されます.
	 */
	public int size() {
		checkClose();
		LeveldbIterator it = null;
		try {
			int ret = 0;
			// Iteratorで削除するので、超遅い.
			if(writeBatchFlag) {
				it = getSnapshot();
				it.first();
			} else {
				it = leveldb.iterator();
			}
			while (it.valid()) {
				ret++;
				it.next();
			}
			return ret;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if(!writeBatchFlag) {
				it.close();
			}
		}
	}

	/**
	 * この処理はLeveMapでは何もしません.
	 * 
	 * @param set
	 *            例外が発生します.
	 */
	public void getAllKey(Set set) {
		throw new LeveldbException("Not supported.");
	}

	/**
	 * この処理はLeveMapでは何もしません.
	 * 
	 * @param set
	 *            例外が発生します.
	 */
	public void getAllValues(Set set) {
		throw new LeveldbException("Not supported.");
	}

	/**
	 * この処理はLeveMapでは何もしません.
	 * 
	 * @return String 空文字が返却されます.
	 */
	public String toString() {
		checkClose();
		// 何もしない.
		return "";
	}

	/**
	 * 現在オープン中のLeveldbパス名を取得.
	 * 
	 * @return String Leveldbパス名が返却されます.
	 */
	public String getPath() {
		checkClose();
		return leveldb.getPath();
	}

	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public LevelMapIterator iterator() {
		return _iterator(false, null, null);
	}
	
	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param twoKey
	 *            対象のセカンドキーを設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public LevelMapIterator iterator(Object key, Object twoKey) {
		return _iterator(false, key, twoKey);
	}

	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public Object iterator(Object key) {
		return _iterator(false, key, null);
	}

	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param keys
	 *            対象のキー群を設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public Object iteratorMultiKey(Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb definition key type is not multi-key.");
		}
		return _iterator(false, keys, null);
	}
	
	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param reverse
	 *            カーソル移動を逆に移動する場合は[true]を設定します.
	 * @param key
	 *            対象のキーを設定します.
	 * @param twoKey
	 *            対象のセカンドキーを設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public LevelMapIterator iterator(boolean reverse, Object key, Object twoKey) {
		return _iterator(reverse, key, twoKey);
	}

	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param reverse
	 *            カーソル移動を逆に移動する場合は[true]を設定します.
	 * @param key
	 *            対象のキーを設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public Object iterator(boolean reverse, Object key) {
		return _iterator(reverse, key, null);
	}

	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param reverse
	 *            カーソル移動を逆に移動する場合は[true]を設定します.
	 * @param keys
	 *            対象のキー群を設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public Object iteratorMultiKey(boolean reverse, Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb definition key type is not multi-key.");
		}
		return _iterator(reverse, keys, null);
	}
	
	/**
	 * snapshotを取得.
	 * 
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public LevelMapIterator snapshot() {
		return _snapshot(false, null, null);
	}
	
	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param twoKey
	 *            対象のセカンドキーを設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public LevelMapIterator snapshot(Object key, Object twoKey) {
		return _snapshot(false, key, twoKey);
	}

	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public Object snapshot(Object key) {
		return _snapshot(false, key, null);
	}

	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param keys
	 *            対象のキー群を設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public Object snapshotMultiKey(Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb definition key type is not multi-key.");
		}
		return _snapshot(false, keys, null);
	}
	
	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param reverse
	 *            カーソル移動を逆に移動する場合は[true]を設定します.
	 * @param key
	 *            対象のキーを設定します.
	 * @param twoKey
	 *            対象のセカンドキーを設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public LevelMapIterator snapshot(boolean reverse, Object key, Object twoKey) {
		return _snapshot(reverse, key, twoKey);
	}

	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param reverse
	 *            カーソル移動を逆に移動する場合は[true]を設定します.
	 * @param key
	 *            対象のキーを設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public LevelMapIterator snapshot(boolean reverse, Object key) {
		return _snapshot(reverse, key, null);
	}

	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @param reverse
	 *            カーソル移動を逆に移動する場合は[true]を設定します.
	 * @param keys
	 *            対象のキー群を設定します.
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public LevelMapIterator snapshotMultiKey(boolean reverse, Object... keys) {
		if (type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb definition key type is not multi-key.");
		}
		return _snapshot(reverse, keys, null);
	}

	// iterator作成.
	protected LevelMapIterator _iterator(boolean reverse, Object key, Object key2) {
		checkClose();
		LevelMapIterator ret = null;
		try {
			ret = new LevelMapIterator(reverse, this, leveldb.iterator());
			return _search(ret, key, key2);
		} catch(LeveldbException le) {
			if(ret != null) {
				ret.close();
			}
			throw le;
		} catch(Exception e) {
			if(ret != null) {
				ret.close();
			}
			throw new LeveldbException(e);
		}
	}

	// snapShort用のIteratorを作成.
	protected LevelMapIterator _snapshot(boolean reverse, Object key, Object key2) {
		checkClose();
		LevelMapIterator ret = null;
		try {
			ret = new LevelMapIterator(reverse, this, leveldb.snapshot());
			return _search(ret, key, key2);
		} catch(LeveldbException le) {
			if(ret != null) {
				ret.close();
			}
			throw le;
		} catch(Exception e) {
			if(ret != null) {
				ret.close();
			}
			throw new LeveldbException(e);
		}
	}
	
	// 指定キーで検索処理.
	protected LevelMapIterator _search(LevelMapIterator ret, Object key, Object key2) {
		Leveldb.search(ret.itr, ret.reverse, type, key, key2);
		return ret;
	}

	/** LevelMapSet. **/
	protected static class LevelMapSet implements Set {
		private LevelMap map;

		public LevelMapSet(LevelMap map) {
			this.map = map;
		}

		public boolean add(Object arg0) {
			map.put(arg0, null);
			return false;
		}

		public boolean addAll(Collection arg0) {
			map.checkClose();
			Iterator it = arg0.iterator();
			while (it.hasNext()) {
				add(it.next());
			}
			return true;
		}

		public void clear() {
			map.clear();
		}

		public boolean contains(Object arg0) {
			return map.containsKey(arg0);
		}

		public boolean containsAll(Collection arg0) {
			map.checkClose();
			Iterator it = arg0.iterator();
			while (it.hasNext()) {
				if (map.containsKey(it.next())) {
					continue;
				}
				return false;
			}
			return true;
		}

		public boolean isEmpty() {
			return map.isEmpty();
		}

		public Iterator<Object> iterator() {
			return map._iterator(false, null, null);
		}

		public boolean remove(Object arg0) {
			return (Boolean) map.remove(arg0);
		}

		public boolean removeAll(Collection arg0) {
			map.checkClose();
			boolean ret = false;
			Iterator it = arg0.iterator();
			while (it.hasNext()) {
				if ((Boolean) map.remove(it.next())) {
					ret = true;
				}
			}
			return ret;
		}

		public boolean retainAll(Collection arg0) {
			throw new LeveldbException("Not supported.");
		}

		public int size() {
			return map.size();
		}

		public Object[] toArray() {
			throw new LeveldbException("Not supported.");
		}

		public Object[] toArray(Object[] arg0) {
			throw new LeveldbException("Not supported.");
		}
	}
	
	/**
	 * LevelMap用Iterator.
	 */
	public class LevelMapIterator extends LevelIterator<Object, Object> {
		LevelMap map;
		LeveldbIterator itr;
		int type;

		/**
		 * コンストラクタ.
		 * 
		 * @param reverse
		 *            逆カーソル移動させる場合は[true]
		 * @param map
		 *            対象の親オブジェクトを設定します.
		 * @param itr
		 *            LeveldbIteratorオブジェクトを設定します.
		 */
		LevelMapIterator(boolean reverse, LevelMap map, LeveldbIterator itr) {
			this.map = map;
			this.itr = itr;
			this.type = map.getType();
			this.reverse = reverse;
		}

		// ファイナライズ.
//		protected void finalize() throws Exception {
//			close();
//		}

		/**
		 * クローズ処理.
		 */
		public void close() {
			super.close();
			if (itr != null) {
				itr.close();
				itr = null;
			}
		}

		@Override
		public boolean isClose() {
			return itr == null || itr.isClose();
		}

		/**
		 * 次の情報が存在するかチェック.
		 * 
		 * @return boolean [true]の場合、存在します.
		 */
		public boolean hasNext() {
			if (map.isClose() || itr == null || !itr.valid()) {
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
			if (map.isClose() || itr == null || !itr.valid()) {
				close();
				throw new NoSuchElementException();
			}
			JniBuffer keyBuf = null;
			try {
				keyBuf = LevelBuffer.key();
				itr.key(keyBuf);
				this.key = LevelId.get(map.getType(), keyBuf);
				Object ret = LevelId.get(type, keyBuf);
				LevelBuffer.clearBuffer(keyBuf, null);
				keyBuf = null;
				if(reverse) {
					itr.before();
				} else {
					itr.next();
				}
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
	}
}
