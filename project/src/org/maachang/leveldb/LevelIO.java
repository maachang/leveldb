package org.maachang.leveldb;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * LeveldbのWriteBatch対応版-Map実装. LevelMapと違うのは、読み込みは、snapShotを
 * 利用することと、書き込みはWriteBatchで書き込む ことです. またsnapShotは、commitおよびrollback処理を実行
 * することで、リセットされます. WriteBatchの書き込みはcommit処理を実行することで Leveldbに反映されます.
 * WriteBatchの書き込みをキャンセルしたい場合は、 rollbackのようにキャンセル処理を実行することで実施できます.
 * 
 * 便利な点としては、書き込みのキャンセルができること snapShotにより、一貫性の読み込みができること.
 * 欠点としては、snapShot作成に時間がかかることで、 読み込み速度が、LevelMapより遅くなることです.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class LevelIO implements Map<Object, Object> {

	/** LevelMapオブジェクト. **/
	protected LevelMap map;

	/** サブ定義. **/
	protected boolean sub = false;

	/** WriteBatchオブジェクト. **/
	protected WriteBatch batch;

	/** LeveldbSnapShot. **/
	protected LeveldbIterator snapShot;

	/** LevelDbマップセット用オブジェクト. **/
	protected LevelIOSet set;

	/** 全クリアーフラグ. **/
	protected boolean allClearFlag = false;

	/** Jniバッファ. **/
	protected JniBuffer _keyBuf;
	protected JniBuffer _valueBuf;

	/**
	 * コンストラクタ. この処理でオープンした場合は、close処理では、Leveldbが クローズされます.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public LevelIO(String name) throws Exception {
		this(name, null);
	}

	/**
	 * コンストラクタ. この処理でオープンした場合は、close処理では、Leveldb本体が クローズされます.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 */
	public LevelIO(String name, LevelOption option) throws Exception {
		this.map = new LevelMap(name, option);
		this.set = null;
		this.batch = null;
		this.snapShot = null;
		this.sub = true;

		createBuffer();
	}

	/**
	 * コンストラクタ. この処理では、close処理を行ったとしても、元のLeveldb本体は クローズされません.
	 * 
	 * @param map
	 *            対象のLevelMapを設定します.
	 */
	public LevelIO(LevelMap map) {
		this.map = map;
		this.set = null;
		this.batch = null;
		this.snapShot = null;
		this.sub = false;

		createBuffer();
	}

	/**
	 * デストラクタ.
	 */
	protected void finalize() throws Exception {
		close();
	}

	/** チェック処理. **/
	private void check() {
		if (map == null) {
			throw new LeveldbException("オブジェクトは既にクリアされています");
		}
	}

	/** バッファ生成. **/
	protected void createBuffer() {
		try {
			_keyBuf = LevelBuffer.key();
			_valueBuf = LevelBuffer.value();
		} catch (Exception e) {
			throw new LeveldbException(e);
		}
	}

	/** キーバッファを取得. **/
	public JniBuffer key() {
		return _keyBuf;
	}

	/** キーバッファを取得. **/
	public JniBuffer key(Object key, Object twoKey) throws Exception {
		LevelId.buf(map.type, _keyBuf, key, twoKey);
		return _keyBuf;
	}

	/** 要素バッファを取得. **/
	public JniBuffer value() {
		return _valueBuf;
	}

	/** 要素バッファを取得. **/
	public JniBuffer value(Object value) throws Exception {
		LevelValues.encode(_valueBuf, value);
		return _valueBuf;
	}

	/** バッチ情報を作成. **/
	private void writeBatch() {
		if (batch == null) {
			batch = new WriteBatch();
		}
	}

	/** Snapshotを作成. **/
	private void getSnapshot() {
		if (snapShot == null) {
			snapShot = map.leveldb.snapShot();
		}
	}

	/**
	 * WriteBatchオブジェクトを取得.
	 * 
	 * @return WriteBatch WriteBatchオブジェクトが返却されます.
	 */
	public WriteBatch getWriteBatch() {
		writeBatch();
		return batch;
	}

	/**
	 * オブジェクトクローズ.
	 */
	public void close() {
		if (batch != null) {
			batch.close();
			batch = null;
		}
		if (snapShot != null) {
			snapShot.close();
			snapShot = null;
		}
		if (sub) {
			map.close();
		}
		allClearFlag = false;
		map = null;
	}

	/**
	 * WriteBatch内容を反映.
	 * 
	 * @exception Exception
	 *                例外.
	 */
	public void commit() throws Exception {
		check();

		// 全データ削除処理.
		if (allClearFlag) {
			map.clear();
			allClearFlag = false;
		}
		// バッチ反映.
		if (batch != null) {
			batch.flush(map.leveldb);
			batch.close();
			batch = null;
		}
		// スナップショットをクリア.
		if (snapShot != null) {
			snapShot.close();
			snapShot = null;
		}
	}

	/**
	 * WriteBatch内容を破棄.
	 * 
	 * @exception Exception
	 *                例外.
	 */
	public void rollback() throws Exception {
		check();

		// バッチクリア.
		if (batch != null) {
			batch.close();
			batch = null;
		}
		// スナップショットをクリア.
		if (snapShot != null) {
			snapShot.close();
			snapShot = null;
		}
		allClearFlag = false;
	}

	/**
	 * クローズしているかチェック.
	 * 
	 * @return boolean [true]の場合、クローズしています.
	 */
	public boolean isClose() {
		return map == null;
	}

	/**
	 * Leveldbのオプションを取得.
	 * 
	 * @return LevelOption オプションが返却されます.
	 */
	public LevelOption getOption() {
		check();
		return map.leveldb.getOption();
	}

	/**
	 * Leveldbキータイプを取得.
	 * 
	 * @return int キータイプが返却されます.
	 */
	public int getType() {
		check();
		return map.type;
	}

	/**
	 * 情報クリア. ※Iteratorで処理をするので、件数が多い場合は、処理に時間がかかります.
	 * この処理を呼び出すと、対象のLeveldbに登録されている すべての要素をすべてクリアします.
	 */
	public void clear() {
		check();
		allClearFlag = true;
		if (batch != null) {
			batch.close();
			batch = null;
		}
	}

	/**
	 * 指定Map情報の内容をすべてセット.
	 * 
	 * @param toMerge
	 *            追加対象のMapを設定します.
	 */
	public void putAll(Map toMerge) {
		check();
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
		check();

		// Iteratorで、存在するまでチェック(超遅い).
		JniBuffer v = null;
		try {
			getSnapshot();
			snapShot.first();
			v = value();
			if (value == null) {
				while (snapShot.valid()) {
					v.clear();
					if (snapShot.value(v) > 0) {
						if (LevelValues.decode(v) == null) {
							return true;
						}
					}
					snapShot.next();
				}
			} else {
				while (snapShot.valid()) {
					v.clear();
					if (snapShot.value(v) > 0) {
						if (value.equals(LevelValues.decode(v))) {
							return true;
						}
					}
					snapShot.next();
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
		check();
		throw new LeveldbException("サポートされていません");
	}

	/**
	 * この処理はLeveMapでは何もしません. return 例外が返却されます.
	 */
	public Collection values() {
		check();
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
	public Object put(Object key, Object twoKey, Object value) {
		check();
		if (value != null && value instanceof LevelMap) {
			throw new LeveldbException("要素にLevelMap要素は設定できません");
		}
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			writeBatch();
			keyBuf = key(key, twoKey);
			if (value instanceof JniBuffer) {
				batch.put(keyBuf, (JniBuffer) value);
			} else {
				valBuf = value(value);
				batch.put(keyBuf, valBuf);
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
	public Object put(Object key, Object value) {
		check();
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
		if (map.type != LevelOption.TYPE_MULTI) {
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
	public boolean containsKey(Object key, Object twoKey) {
		check();
		JniBuffer keyBuf = null;
		JniBuffer outBuf = null;
		try {

			// snapShotで検索.
			getSnapshot();
			keyBuf = key(key, twoKey);
			snapShot.seek(keyBuf);

			// 条件が存在する場合.
			if (snapShot.valid()) {
				outBuf = value();
				snapShot.key(outBuf);
				return JniIO.equals(keyBuf.address, keyBuf.position,
						outBuf.address, outBuf.position);
			}
			return false;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (keyBuf != null) {
				keyBuf.clear(true);
			}
			if (outBuf != null) {
				outBuf.clear();
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
		if (map.type != LevelOption.TYPE_MULTI) {
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
	public boolean getBuffer(JniBuffer buf, Object key, Object twoKey) {
		check();
		boolean ret = false;
		JniBuffer keyBuf = null;
		try {

			// snapShotで検索.
			getSnapshot();
			if (key instanceof JniBuffer) {
				if (twoKey == null) {
					keyBuf = (JniBuffer) key;
				} else {
					throw new LeveldbException(
							"key=jniBufferに対して、twoKeyが指定されています");
				}
			} else {
				keyBuf = key(key, twoKey);
			}
			snapShot.seek(keyBuf);

			// 条件が存在する場合.
			if (snapShot.valid()) {
				buf.clear();
				snapShot.key(buf);

				// 対象キーが正しい場合.
				if (JniIO.equals(keyBuf.address, keyBuf.position, buf.address,
						buf.position)) {

					buf.clear();
					snapShot.value(buf);
					ret = true;
				}
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
	public boolean getBufferMultiKey(JniBuffer buf, Object... keys) {
		if (map.type != LevelOption.TYPE_MULTI) {
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
	public Object get(Object key, Object twoKey) {
		check();
		JniBuffer buf = null;
		try {
			buf = value();
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
		if (map.type != LevelOption.TYPE_MULTI) {
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
	public boolean remove(Object key, Object twoKey) {
		check();
		JniBuffer keyBuf = null;
		try {
			writeBatch();
			keyBuf = key(key, twoKey);
			batch.remove(keyBuf);
			return Boolean.TRUE;
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
		if (map.type != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb定義のキータイプはマルチキーではありません");
		}
		return remove(keys, null);
	}

	/**
	 * 情報が空かチェック. return boolean [false]が返却されます.
	 */
	public boolean isEmpty() {
		check();
		try {
			// 1件以上のIteratorが存在する場合は[false].
			getSnapshot();
			snapShot.first();
			if (snapShot.valid()) {
				return false;
			}
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
	public Set keySet() {
		check();
		if (set == null) {
			set = new LevelIOSet(this);
		}
		return set;
	}

	/**
	 * 登録データ数を取得. ※Iteratorでカウントするので、件数が多い場合は、処理に時間がかかります. return int
	 * 登録データ数が返却されます.
	 */
	public int size() {
		check();
		try {
			int ret = 0;

			// Iteratorで削除するので、超遅い.
			getSnapshot();
			snapShot.first();
			while (snapShot.valid()) {
				ret++;
				snapShot.next();
			}
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
	public void getAllKey(Set set) {
		throw new LeveldbException("サポートされていません");
	}

	/**
	 * この処理はLeveMapでは何もしません.
	 * 
	 * @param set
	 *            例外が発生します.
	 */
	public void getAllValues(Set set) {
		throw new LeveldbException("サポートされていません");
	}

	/**
	 * この処理はLeveMapでは何もしません.
	 * 
	 * @return String 空文字が返却されます.
	 */
	public String toString() {

		// 何もしない.
		return "";
	}

	/**
	 * 現在オープン中のLeveldbパス名を取得.
	 * 
	 * @return String Leveldbパス名が返却されます.
	 */
	public String getPath() {
		check();
		return map.getPath();
	}

	/**
	 * LevelMapIteratorを取得.
	 * 
	 * @return LevelMapIterator LevelMapIteratorが返却されます.
	 */
	public LevelMapIterator iterator() {
		return map.snapshot();
	}

	/** LevelIOSet. **/
	protected static class LevelIOSet implements Set {
		private LevelIO map;

		public LevelIOSet(LevelIO map) {
			this.map = map;
		}

		public boolean add(Object arg0) {
			map.put(arg0, null);
			return false;
		}

		public boolean addAll(Collection arg0) {
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
			return map.iterator();
		}

		public boolean remove(Object arg0) {
			return (Boolean) map.remove(arg0);
		}

		public boolean removeAll(Collection arg0) {
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
			throw new LeveldbException("サポートされていません");
		}

		public int size() {
			return map.size();
		}

		public Object[] toArray() {
			throw new LeveldbException("サポートされていません");
		}

		public Object[] toArray(Object[] arg0) {
			throw new LeveldbException("サポートされていません");
		}
	}
}
