package org.maachang.leveldb;

import java.util.NoSuchElementException;

/**
 * Levelキュー情報.
 */
public class LevelQueue extends CommitRollback {
	protected Time12SequenceId sequenceId;
	
	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 */
	public LevelQueue(String name) {
		this(0, name, null);
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
	public LevelQueue(String name, LevelOption option) {
		this(0, name, option);
	}

	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
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
	 * writeBatchを無効にして生成します.
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
		Leveldb db  = new Leveldb(name, option);
		super.init(db, true, false);
		this.sequenceId = new Time12SequenceId(machineId);
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを有効にして生成します.
	 * 
	 * @param db
	 *            Leveldbを設定します.
	 */
	public LevelQueue(Leveldb db) {
		this(true, 0, db);
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを有効にして生成します.
	 * 
	 * @param machineId
	 *            マシンIDを設定します.
	 * @param db
	 *            Leveldbを設定します.
	 */
	public LevelQueue(int machineId, Leveldb db) {
		this(true, machineId, db);
	}
	
	/**
	 * コンストラクタ.
	 * 
	 * @param writeBatch
	 *            writeBatchを有効にする場合は[true].
	 * @param db
	 *            Leveldbを設定します.
	 */
	public LevelQueue(boolean writeBatch, Leveldb db) {
		this(writeBatch, 0, db);
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
	public LevelQueue(boolean writeBatch, int machineId, Leveldb db) {
		if(db.getType() != LevelOption.TYPE_FREE) {
			throw new LeveldbException("The key type of the opened Leveldb is not TYPE_FREE.");
		}
		if(writeBatch) {
			super.init(db, false, true);
		} else {
			super.init(db, true, false);
		}
		this.sequenceId = new Time12SequenceId(machineId);
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを有効にして生成します.
	 * 
	 * @param queue
	 */
	public LevelQueue(LevelQueue queue) {
		this(true, queue.sequenceId.getMachineId(), queue.leveldb);
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
			if(o instanceof JniBuffer) {
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, (JniBuffer)o);
				} else {
					leveldb.put(keyBuf, (JniBuffer)o);
				}
			} else {
				valBuf = LevelBuffer.value(o);
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, valBuf);
				} else {
					leveldb.put(keyBuf, valBuf);
				}
			}
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
		Object ret = null;
		LeveldbIterator itr = null;
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			if(writeBatchFlag) {
				itr = getSnapshot();
				if (itr.valid()) {
					keyBuf = LevelBuffer.key();
					valBuf = LevelBuffer.value();
					itr.key(keyBuf);
					itr.value(valBuf);
					itr.next();
					writeBatch().remove(keyBuf);
					if (out != null) {
						out[0] = Time12SequenceId.toString(keyBuf.getBinary());
					}
					ret = LevelValues.decode(valBuf);
				}
			} else {
				itr = leveldb.iterator();
				while(true) {
					ret = null;
					if (itr.valid()) {
						keyBuf = LevelBuffer.key();
						valBuf = LevelBuffer.value();
						itr.key(keyBuf);
						itr.value(valBuf);
						if (out != null) {
							out[0] = Time12SequenceId.toString(keyBuf.getBinary());
						}
						ret = LevelValues.decode(valBuf);
						if(!leveldb.remove(keyBuf)) {
							// 削除に失敗した場合はやり直す.
							continue;
						}
					}
					itr.close();
					break;
				}
			}
			itr = null;
			return ret;
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
		if(writeBatchFlag) {
			try {
				LeveldbIterator snapShot = getSnapshot();
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
		return leveldb.isEmpty();
	}
	
	/**
	 * Queue用Iterator.
	 */
	public static class LevelQueueIterator implements LevelIterator<KeyValue<String, Object>> {
		private KeyValue<String, Object> keyValue = new KeyValue<String, Object>();
		private LeveldbIterator itr = null;
		private LevelQueue queue = null;
		
		LevelQueueIterator(LevelQueue q, LeveldbIterator i, byte[] key) {
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
			queue = q;
			itr = i;
		}

		// ファイナライズ.
		protected void finalize() throws Exception {
			close();
		}

		/**
		 * クローズ.
		 * 利用後はクローズ処理を行います.
		 */
		@Override
		public void close() {
			LeveldbIterator i = itr; itr = null;
			if (i != null) {
				i.close();
			}
			keyValue = null;
		}

		/**
		 * 次の情報が存在するかチェック.
		 * @return
		 */
		@Override
		public boolean hasNext() {
			if (queue.isClose() || itr == null || !itr.valid()) {
				close();
				return false;
			}
			return true;
		}

		/**
		 * 次の情報を取得.
		 * @return
		 */
		@Override
		public KeyValue<String, Object> next() {
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
				keyValue.set(Time12SequenceId.toString(keyBuf.getBinary()),
					LevelValues.decode(valBuf));
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
		public boolean isReverse() {
			return false;
		}
	}

	/**
	 * iteratorを取得.
	 * 
	 * @param key
	 * @return
	 */
	public LevelQueueIterator iterator() {
		return new LevelQueueIterator(this, leveldb.snapshot(), null);
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
		Time12SequenceId.first(key);
		return new LevelQueueIterator(this, leveldb.snapshot(), key);
	}
}
