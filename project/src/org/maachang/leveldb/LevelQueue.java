package org.maachang.leveldb;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.maachang.leveldb.util.Flag;

/**
 * Levelキュー情報.
 */
public class LevelQueue {
	protected Leveldb leveldb;
	protected Time12SequenceId sequenceId;
	protected Flag closeFlag = new Flag();
	
	/**
	 * コンストラクタ.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 */
	public LevelQueue(String name) {
		this(0, name, null);
	}
	
	/**
	 * コンストラクタ.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 */
	public LevelQueue(String name, LevelOption option) {
		this(0, name, option);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param machineId
	 *            マシンIDを設定します.
	 * @param name
	 *            対象のデータベース名を設定します.
	 */
	public LevelQueue(int machineId, String name) {
		this(machineId, name, null);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param machineId
	 *            マシンIDを設定します.
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 */
	public LevelQueue(int machineId, String name, LevelOption option) {
		// keyタイプは free(Time12SequenceId).
		option.type = LevelOption.TYPE_FREE;
		this.leveldb = new Leveldb(name, option);
		this.sequenceId = new Time12SequenceId(machineId);
		this.closeFlag.set(false);
	}
	
	/**
	 * コンストラクタ.
	 * 
	 * @param db
	 *            対象のデータベース名を設定します.
	 */
	public LevelQueue(Leveldb db) {
		this(0, db);
	}
	
	/**
	 * コンストラクタ.
	 * 
	 * @param machineId
	 *            マシンIDを設定します.
	 * @param db
	 *            対象のデータベース名を設定します.
	 */
	public LevelQueue(int machineId, Leveldb db) {
		if(db.getType() != LevelOption.TYPE_FREE) {
			throw new LeveldbException("The key type of the opened Leveldb is not TYPE_FREE.");
		}
		this.leveldb = db;
		this.sequenceId = new Time12SequenceId(machineId);
		this.closeFlag.set(false);
	}

	// ファイナライズ.
	protected void finalize() throws Exception {
		this.close();
	}

	/**
	 * クローズ処理.
	 */
	public void close() {
		if(!closeFlag.setToGetBefore(true)) {
			Leveldb db = leveldb; leveldb = null;
			if (db != null) {
				db.close();
				db = null;
			}
		}
	}

	/**
	 * クローズしているかチェック.
	 * 
	 * @return boolean [true]の場合、クローズしています.
	 */
	public final boolean isClose() {
		return closeFlag.get();
	}

	// クローズチェック.
	protected final void checkClose() {
		if (closeFlag.get()) {
			throw new LeveldbException("Already closed.");
		}
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
	 * Levedbオープンオプションを取得.
	 * 
	 * @return LevelOption オプション情報が返却されます.
	 */
	public LevelOption getOption() {
		checkClose();
		return leveldb.getOption();
	}

	/**
	 * 最後に追加.
	 * 
	 * @param o
	 * @return
	 */
	public byte[] add(Object o) {
		checkClose();
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			byte[] key = sequenceId.next();
			keyBuf = LevelBuffer.key(LevelOption.TYPE_FREE, key);
			valBuf = LevelBuffer.value(o);
			leveldb.put(keyBuf, valBuf);
			return key;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, valBuf);
		}
	}

	/**
	 * 先頭の情報を取得して削除.
	 * 
	 * @param out
	 *            key情報を取得する場合に設定します.
	 * @return
	 */
	public Object get() {
		return get(null);
	}

	/**
	 * 先頭の情報を取得して削除.
	 * 
	 * @param out
	 *            key情報を取得する場合に設定します.
	 * @return
	 */
	public Object get(String[] out) {
		checkClose();
		LeveldbIterator itr = null;
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			itr = leveldb.snapShot();
			if (itr.valid()) {
				keyBuf = LevelBuffer.key();
				valBuf = LevelBuffer.value();
				itr.key(keyBuf);
				itr.value(valBuf);
				itr.close();
				itr = null;
				leveldb.remove(keyBuf);
				if (out != null) {
					out[0] = Time12SequenceId.toString(keyBuf.getBinary());
				}
				return LevelValues.decode(valBuf);
			}
			itr.close();
			itr = null;
			return null;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (itr != null) {
				itr.close();
			}
			LevelBuffer.clearBuffer(keyBuf, valBuf);
		}
	}

	/**
	 * 情報が空かチェック.
	 * 
	 * @return boolean [true]の場合、空です.
	 */
	public boolean isEmpty() {
		checkClose();
		return isEmpty();
	}

	// LevelQueue用Iteratorデータ取得用要素.
	public class KeyValue {
		private String key;
		private Object value;

		KeyValue() {
		}

		KeyValue(String k, Object v) {
			key = k;
			value = v;
		}

		public void create(String k, Object v) {
			key = k;
			value = v;
		}

		public String getKey() {
			return key;
		}

		public Object getValue() {
			return value;
		}
	}

	// LevelQueue用Iterator.
	public class LevelQueueIterator implements Iterator<KeyValue> {
		private KeyValue keyValue = new KeyValue();
		private LeveldbIterator itr = null;
		private LevelQueue queue = null;
		LevelQueueIterator(LevelQueue q, byte[] key) {
			LeveldbIterator i = q.leveldb.snapShot();
			if (key != null) {
				JniBuffer buf = null;
				try {
					buf = LevelBuffer.key(LevelOption.TYPE_FREE, key);
					i.seek(buf);
				} catch (Exception e) {
					if (i != null) {
						i.close();
					}
					throw new LeveldbException(e);
				} finally {
					LevelBuffer.clearBuffer(buf, null);
				}
			}
			itr = i;
			queue = q;
		}

		protected void finalize() throws Exception {
			close();
		}

		public void close() {
			LeveldbIterator i = itr; itr = null;
			if (i != null) {
				i.close();
			}
			keyValue = null;
		}

		@Override
		public boolean hasNext() {
			if (queue.isClose() || itr == null || !itr.valid()) {
				close();
				return false;
			}
			return true;
		}

		@Override
		public KeyValue next() {
			if (queue.isClose() || itr == null || !itr.valid()) {
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
				itr.next();
				if (!itr.valid()) {
					close();
				}
				keyValue.create(Time12SequenceId.toString(keyBuf.getBinary()), LevelValues.decode(valBuf));
				LevelBuffer.clearBuffer(keyBuf, valBuf);
				keyBuf = null; valBuf = null;
				return keyValue;
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			} finally {
				LevelBuffer.clearBuffer(keyBuf, valBuf);
			}
		}

		@Override
		public void remove() {
			if (queue.isClose() || itr == null || !itr.valid()) {
				close();
				return;
			}
			JniBuffer keyBuf = null;
			try {
				keyBuf = LevelBuffer.key();
				itr.key(keyBuf);
				queue.leveldb.remove(keyBuf);
				LevelBuffer.clearBuffer(keyBuf, null);
				keyBuf = null;
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			} finally {
				LevelBuffer.clearBuffer(keyBuf, null);
			}
		}
	}

	/**
	 * iteratorを取得.
	 * 
	 * @param key
	 * @return
	 */
	public LevelQueueIterator iterator() {
		return new LevelQueueIterator(this, null);
	}

	/**
	 * iteratorを取得.
	 * 
	 * @param time
	 *            開始位置のミリ秒からのunix時間を設定します.
	 * @return
	 */
	public LevelQueueIterator iterator(long time) {
		return iterator(Time12SequenceId.createId(0, time, 0));
	}

	/**
	 * iteratorを取得.
	 * 
	 * @param key
	 *            開始位置のキー情報を設定します.
	 * @return
	 */
	public LevelQueueIterator iterator(String key) {
		return iterator(Time12SequenceId.toBinary(key));
	}

	/**
	 * iteratorを取得.
	 * 
	 * @param key
	 *            開始位置のキー情報を設定します.
	 * @return
	 */
	public LevelQueueIterator iterator(byte[] key) {
		key[8] = 0;
		key[9] = 0;
		key[10] = 0;
		key[11] = 0;
		return new LevelQueueIterator(this, key);
	}
}
