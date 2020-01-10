package org.maachang.leveldb.operator;

import java.util.NoSuchElementException;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.JniIO;
import org.maachang.leveldb.KeyValue;
import org.maachang.leveldb.LevelBuffer;
import org.maachang.leveldb.LevelId;
import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.LevelValues;
import org.maachang.leveldb.Leveldb;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.LeveldbIterator;
import org.maachang.leveldb.Time12SequenceId;
import org.maachang.leveldb.WriteBatch;

/**
 * キー情報をシーケンスIDで管理.
 */
public class LevelSequence extends LevelIndexOperator {
	protected Time12SequenceId sequenceId;
	protected int machineId;

	/**
	 * オペレータタイプ.
	 * @return int オペレータタイプが返却されます.
	 */
	@Override
	public int getOperatorType() {
		return LEVEL_SEQUENCE;
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param machineId
	 *            マシンIDを設定します.
	 */
	public LevelSequence(String name, int machineId) {
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
	public LevelSequence(String name, int machineId, LevelOption option) {
		if(option != null) {
			option.setType(LevelOption.TYPE_FREE);
		} else {
			option = LevelOption.create(LevelOption.TYPE_FREE);
		}
		Leveldb db = new Leveldb(name, option);
		// leveldbをクローズしてwriteBatchで処理しない.
		super.init(null, db, true, false);
		this.sequenceId = new Time12SequenceId(machineId);
		this.machineId = machineId;
		
		// インデックス初期化.
		super.initIndex(null);
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを有効にして生成します.
	 * 
	 * @param seq 親となるオペレータを設定します.
	 */
	public LevelSequence(LevelSequence seq) {
		// leveldbをクローズせずwriteBatchで処理する.
		super.init(seq, seq.leveldb, false, true);
		this.sequenceId = seq.sequenceId;
		this.machineId = seq.machineId;
		
		// インデックス初期化.
		super.initIndex(seq);
	}
	
	// シーケンスIDのキー情報を正しく取得.
	private static final JniBuffer _getKey(Object key)
		throws Exception {
		if(key == null) {
			return null;
		}
		byte[] bkey = null;
		if(key instanceof JniBuffer) {
			throw new LeveldbException("JniBuffer cannot be set for key.");
//			JniBuffer ret = (JniBuffer)key;
//			if(ret.position() != Time12SequenceId.ID_LENGTH) {
//				throw new LeveldbException("Failed to interpret the specified sequence ID.");
//			}
//			return ret;
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
	 * 新しいシーケンスIDで追加.
	 * 
	 * @param value 設定対象の要素を設定します.
	 * @return シーケンスIDが返却されます.
	 */
	public String add(Object value) {
		return this.put(null, value);
	}
	
	/**
	 * 指定したシーケンスIDでデータセット.
	 * 
	 * @param key 対象のシーケンスIDを設定します.
	 * @param value 設定対象の要素を設定します.
	 * @return String シーケンスIDが返却されます.
	 */
	public String put(Object key, Object value) {
		checkClose();
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			if(key == null) {
				key = sequenceId.next();
			} else if(key instanceof String) {
				key = Time12SequenceId.toBinary((String)key);
			}
			keyBuf = _getKey(key);
			if(value instanceof JniBuffer) {
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, (JniBuffer)value);
				} else {
					leveldb.put(keyBuf, (JniBuffer)value);
				}
				super.putIndex(key, null, LevelValues.decode((JniBuffer)value));
			} else {
				valBuf = LevelBuffer.value(value);
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, valBuf);
				} else {
					leveldb.put(keyBuf, valBuf);
				}
				super.putIndex(key, null, value);
			}
			return Time12SequenceId.toString((byte[])key);
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
			keyBuf = _getKey(key);
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
			keyBuf = _getKey(key);
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
	 * @return String シーケンスIDが返却されます.
	 */
	public String remove(Object key) {
		checkClose();
		JniBuffer keyBuf = null;
		try {
			if(key instanceof String) {
				key = Time12SequenceId.toBinary((String)key);
			}
			Object v = get(key);
			keyBuf = _getKey(key);
			if(writeBatchFlag) {
				WriteBatch b = writeBatch();
				b.remove(keyBuf);
			} else {
				leveldb.remove(keyBuf);
			}
			super.removeIndex(key, null, v);
			return Time12SequenceId.toString((byte[])key);
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
	 * マシンIDを取得.
	 * 
	 * @return int マシンIDが返却されます.
	 */
	public int getMachineId() {
		return machineId;
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
	public LevelSequenceIterator iterator() {
		return iterator(false, null);
	}
	
	/**
	 * iterator作成.
	 * @param reverse
	 * @return
	 */
	public LevelSequenceIterator iterator(boolean reverse) {
		return iterator(reverse, null);
	}
	
	/**
	 * iterator作成.
	 * @param reverse
	 * @param key
	 * @return
	 */
	public LevelSequenceIterator iterator(Object key) {
		return iterator(false, key);
	}
	
	/**
	 * iterator作成.
	 * @param reverse
	 * @param key
	 * @return
	 */
	public LevelSequenceIterator iterator(boolean reverse, Object key) {
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
	public LevelSequenceIterator snapshot() {
		return snapshot(false, null);
	}
	
	/**
	 * snapShort用のIteratorを作成.
	 * @param reverse
	 * @return
	 */
	public LevelSequenceIterator snapshot(boolean reverse) {
		return snapshot(reverse, null);
	}
	
	/**
	 * snapShort用のIteratorを作成.
	 * @param reverse
	 * @param key
	 * @return
	 */
	public LevelSequenceIterator snapshot(Object key) {
		return snapshot(false, key);
	}

	/**
	 * snapShort用のIteratorを作成.
	 * @param reverse
	 * @param key
	 * @return
	 */
	public LevelSequenceIterator snapshot(boolean reverse, Object key) {
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
			Leveldb.search(ret.itr, ret.reverse, LevelOption.TYPE_FREE, _getKey(key), null);
		} else if(ret.reverse) {
			ret.itr.last();
		}
		return ret;
	}
	
	/**
	 * LevelSequence用Iterator.
	 */
	public class LevelSequenceIterator extends LevelIterator<KeyValue<String, Object>> {
		LevelSequence seq;
		LeveldbIterator itr;
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
		LevelSequenceIterator(boolean reverse, LevelSequence seq, LeveldbIterator itr) {
			this.seq = seq;
			this.itr = itr;
			this.reverse = reverse;
			this.element = new KeyValue<String, Object>();
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
				key = Time12SequenceId.toString((byte[])ret);
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
				element.set((String)key, val);
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
