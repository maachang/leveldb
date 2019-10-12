package org.maachang.leveldb;

import java.util.NoSuchElementException;

/**
 * キー情報をシーケンスIDで管理.
 */
public class LevelSequenceDb extends CommitRollback {
	protected Time12SequenceId sequenceId = null;
	
	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param machineId
	 *            マシンIDを設定します.
	 */
	public LevelSequenceDb(String name, int machineId) {
		this(name, machineId, null);
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param machineId
	 *            マシンIDを設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 *            オプションのタイプは無視されて、シーケンスID専用のタイプが設定されます.
	 */
	public LevelSequenceDb(String name, int machineId, LevelOption option) {
		if(option != null) {
			option.setType(LevelOption.TYPE_FREE);
		} else {
			option = LevelOption.create(LevelOption.TYPE_FREE);
		}
		sequenceId = new Time12SequenceId(machineId);
		Leveldb db = new Leveldb(name, option);
		// leveldbをクローズしてwriteBatchで処理しない.
		super.init(db, true, false);
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを有効にして生成します.
	 * 
	 * @param db
	 */
	public LevelSequenceDb(LevelSequenceDb db) {
		// leveldbをクローズせずwriteBatchで処理する.
		super.init(db.leveldb, false, true);
		this.sequenceId = db.sequenceId;
	}
	
	/**
	 * コンストラクタ.
	 * 
	 * @param writeBatch
	 *            writeBatchを有効にする場合は[true].
	 * @param machineId
	 *            マシンIDを設定します.
	 * @param db
	 *            Leveldbを設定します.
	 */
	public LevelSequenceDb(boolean writeBatch, int machineId, Leveldb db) {
		if(db.getType() != LevelOption.TYPE_FREE) {
			throw new LeveldbException("The key type of the opened Leveldb is not TYPE_FREE.");
		}
		if(writeBatch) {
			// leveldbをクローズしてwriteBatchで処理しない.
			super.init(db, false, true);
		} else {
			// leveldbをクローズしてwriteBatchで処理しない.
			super.init(db, true, false);
		}
		this.sequenceId = new Time12SequenceId(machineId);
	}
	
	/**
	 * 新しいシーケンスIDで追加.
	 * 
	 * @param value 設定対象の要素を設定します.
	 * @return シーケンスIDが返却されます.
	 */
	public String add(Object value) {
		checkClose();
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			byte[] key = sequenceId.next();
			keyBuf = LevelBuffer.key(LevelOption.TYPE_FREE, key);
			if(value instanceof JniBuffer) {
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, (JniBuffer)value);
				} else {
					leveldb.put(keyBuf, (JniBuffer)value);
				}
			} else {
				valBuf = LevelBuffer.value(value);
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, valBuf);
				} else {
					leveldb.put(keyBuf, valBuf);
				}
			}
			return Time12SequenceId.toString(key);
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, valBuf);
		}
	}
	
	// シーケンスIDのキー情報を正しく取得.
	private static final JniBuffer getKey(Object key)
		throws Exception {
		if(key == null) {
			return null;
		}
		byte[] bkey = null;
		if(key instanceof JniBuffer) {
			JniBuffer ret = (JniBuffer)key;
			if(ret.position() != Time12SequenceId.ID_LENGTH) {
				throw new LeveldbException("Failed to interpret the specified sequence ID.");
			}
			return ret;
		} else if(key instanceof byte[]) {
			bkey = (byte[])key;
		} else if(key instanceof String) {
			bkey = Time12SequenceId.toBinary((String)key);
		}
		if(bkey == null || bkey.length != Time12SequenceId.ID_LENGTH) {
			throw new LeveldbException("Failed to interpret the specified sequence ID.");
		}
		return LevelBuffer.key(LevelOption.TYPE_FREE, bkey);
	}
	
	/**
	 * 指定したシーケンスIDでデータセット.
	 * 
	 * @param key 対象のシーケンスIDを設定します.
	 * @param value 設定対象の要素を設定します.
	 */
	public void put(Object key, Object value) {
		checkClose();
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			keyBuf = getKey(key);
			if(value instanceof JniBuffer) {
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, (JniBuffer)value);
				} else {
					leveldb.put(keyBuf, (JniBuffer)value);
				}
			} else {
				valBuf = LevelBuffer.value(value);
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, valBuf);
				} else {
					leveldb.put(keyBuf, valBuf);
				}
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
	 *            対象のシーケンスIDを設定します.
	 * @return boolean [true]の場合、存在します.
	 */
	public boolean contains(Object key) {
		checkClose();
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			keyBuf = getKey(key);
			if(writeBatchFlag) {
				LeveldbIterator snapshot = getSnapshot();
				snapshot.seek(keyBuf);
				if (snapshot.valid()) {
					valBuf = LevelBuffer.value();
					snapshot.key(valBuf);
					return JniIO.equals(keyBuf.address(), keyBuf.position(),
						valBuf.address(), valBuf.position());
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
	 * 指定キー情報に対する要素を取得.
	 * 
	 * @param buf
	 *            対象の要素格納用バッファを設定します.
	 * @param key
	 *            対象のシーケンスIDを設定します.
	 * @return boolean [true]の場合、セットされました.
	 */
	public boolean getBuffer(JniBuffer buf, Object key) {
		checkClose();
		boolean ret = false;
		JniBuffer keyBuf = null;
		try {
			keyBuf = getKey(key);
			if(writeBatchFlag) {
				LeveldbIterator snapshot = getSnapshot();
				snapshot.seek(keyBuf);
				// 条件が存在する場合.
				if (snapshot.valid()) {
					snapshot.key(buf);
					// 対象キーが正しい場合.
					if (JniIO.equals(keyBuf.address(), keyBuf.position(),
						buf.address(), buf.position())) {
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
	 * @param key
	 *            対象のシーケンスIDを設定します.
	 * @return Object 対象の要素が返却されます.
	 */
	public Object get(Object key) {
		checkClose();
		JniBuffer buf = null;
		try {
			buf = LevelBuffer.value();
			if (getBuffer(buf, key)) {
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
	 * 指定キーの情報を削除.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return boolean 削除できた場合[true]が返却されます.
	 */
	public boolean remove(Object key) {
		checkClose();
		JniBuffer keyBuf = null;
		try {
			keyBuf = getKey(key);
			if(writeBatchFlag) {
				WriteBatch b = writeBatch();
				b.remove(keyBuf);
				return true;
			}
			return leveldb.remove(keyBuf);
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, null);
		}
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
	 * 現在オープン中のLeveldbパス名を取得.
	 * 
	 * @return String Leveldbパス名が返却されます.
	 */
	public String getPath() {
		checkClose();
		return leveldb.getPath();
	}
	
	/**
	 * iterator作成.
	 * @return
	 */
	protected LevelSequenceIterator iterator() {
		return iterator(false, null);
	}
	
	/**
	 * iterator作成.
	 * @param reverse
	 * @return
	 */
	protected LevelSequenceIterator iterator(boolean reverse) {
		return iterator(reverse, null);
	}
	
	/**
	 * iterator作成.
	 * @param reverse
	 * @param key
	 * @return
	 */
	protected LevelSequenceIterator iterator(Object key) {
		return iterator(false, key);
	}
	
	/**
	 * iterator作成.
	 * @param reverse
	 * @param key
	 * @return
	 */
	protected LevelSequenceIterator iterator(boolean reverse, Object key) {
		checkClose();
		LevelSequenceIterator ret = null;
		try {
			ret = new LevelSequenceIterator(reverse, this, leveldb.iterator());
			return _search(ret, key);
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
	
	/**
	 * snapShort用のIteratorを作成.
	 * @return
	 */
	protected LevelSequenceIterator snapshot() {
		return snapshot(false, null);
	}
	
	/**
	 * snapShort用のIteratorを作成.
	 * @param reverse
	 * @return
	 */
	protected LevelSequenceIterator snapshot(boolean reverse) {
		return snapshot(reverse, null);
	}
	
	/**
	 * snapShort用のIteratorを作成.
	 * @param reverse
	 * @param key
	 * @return
	 */
	protected LevelSequenceIterator snapshot(Object key) {
		return snapshot(false, key);
	}

	/**
	 * snapShort用のIteratorを作成.
	 * @param reverse
	 * @param key
	 * @return
	 */
	protected LevelSequenceIterator snapshot(boolean reverse, Object key) {
		checkClose();
		LevelSequenceIterator ret = null;
		try {
			ret = new LevelSequenceIterator(reverse, this, leveldb.snapshot());
			return _search(ret, key);
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
	protected LevelSequenceIterator _search(LevelSequenceIterator ret, Object key)
		throws Exception {
		if(key != null) {
			Leveldb.search(ret.itr, ret.reverse, LevelOption.TYPE_FREE, getKey(key), null);
		} else if(ret.reverse) {
			ret.itr.last();
		}
		return ret;
	}
	
	/**
	 * LevelSequence用Iterator.
	 */
	public class LevelSequenceIterator implements LevelIterator<KeyValue<String, Object>> {
		LevelSequenceDb seq;
		LeveldbIterator itr;
		boolean reverse;
		KeyValue<String, Object> element;

		/**
		 * コンストラクタ.
		 * 
		 * @param reverse
		 *            逆カーソル移動させる場合は[true]
		 * @param seq
		 *            対象の親オブジェクトを設定します.
		 * @param itr
		 *            LeveldbIteratorオブジェクトを設定します.
		 */
		LevelSequenceIterator(boolean reverse, LevelSequenceDb seq, LeveldbIterator itr) {
			this.seq = seq;
			this.itr = itr;
			this.reverse = reverse;
			this.element = new KeyValue<String, Object>();
		}

		// ファイナライズ.
		protected void finalize() throws Exception {
			close();
		}

		/**
		 * クローズ処理.
		 */
		public void close() {
			if (itr != null) {
				itr.close();
				itr = null;
			}
		}
		
		/**
		 * 逆カーソル移動かチェック.
		 * @return
		 */
		public boolean isReverse() {
			return reverse;
		}

		/**
		 * 次の情報が存在するかチェック.
		 * 
		 * @return boolean [true]の場合、存在します.
		 */
		public boolean hasNext() {
			if (seq.isClose() || itr == null || !itr.valid()) {
				close();
				return false;
			}
			return true;
		}

		/**
		 * 次の要素を取得.
		 * 
		 * @return String 次の要素が返却されます.
		 */
		public KeyValue<String, Object> next() {
			if (seq.isClose() || itr == null || !itr.valid()) {
				close();
				throw new NoSuchElementException();
			}
			JniBuffer keyBuf = null;
			JniBuffer valBuf = null;
			try {
				keyBuf = LevelBuffer.key();
				valBuf = LevelBuffer.value();
				itr.key(keyBuf);
				itr.value(valBuf);
				Object ret = LevelId.get(LevelOption.TYPE_FREE, keyBuf);
				Object val = LevelValues.decode(valBuf);
				LevelBuffer.clearBuffer(keyBuf, valBuf);
				keyBuf = null;
				valBuf = null;
				if(reverse) {
					itr.before();
				} else {
					itr.next();
				}
				if(!itr.valid()) {
					close();
				}
				element.set(Time12SequenceId.toString((byte[])ret), val);
				return element;
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			} finally {
				LevelBuffer.clearBuffer(keyBuf, valBuf);
			}
		}
	}
}
