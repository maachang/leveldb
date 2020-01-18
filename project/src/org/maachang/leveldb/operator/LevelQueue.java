package org.maachang.leveldb.operator;

import java.util.NoSuchElementException;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.LevelBuffer;
import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.LevelValues;
import org.maachang.leveldb.Leveldb;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.LeveldbIterator;
import org.maachang.leveldb.Time12SequenceId;

/**
 * Levelキュー情報.
 */
public class LevelQueue extends LevelOperator {
	protected Time12SequenceId sequenceId;
	protected int machineId;
	
	/**
	 * オペレータタイプ.
	 * @return int オペレータタイプが返却されます.
	 */
	@Override
	public int getOperatorType() {
		return LEVEL_QUEUE;
	}
	
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
		option.setType(LevelOption.TYPE_FREE);
		Leveldb db  = new Leveldb(name, option);
		// leveldbをクローズしてwriteBatchで処理しない.
		super.init(null, db, true, false);
		this.machineId = machineId;
		this.sequenceId = new Time12SequenceId(machineId);
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを有効にして生成します.
	 * 
	 * @param queue LevelQueueを設定します.
	 */
	public LevelQueue(LevelQueue queue) {
		// leveldbをクローズしてwriteBatchで処理しない.
		super.init(queue, queue.leveldb, false, true);
		this.machineId = queue.machineId;
		this.sequenceId = new Time12SequenceId(queue.machineId);
	}
	
	/**
	 * 最後に追加.
	 * 
	 * @param o
	 * @return シーケンスIDが返却されます.
	 */
	public String add(Object o) {
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
			return Time12SequenceId.toString(key);
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
					if (out != null && out.length >= 1) {
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
						if (out != null && out.length >= 1) {
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
				LeveldbIterator snapshot = getSnapshot();
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
	 * Queue用Iterator.
	 */
	public static class LevelQueueIterator extends LevelIterator<String, Object> {
		private LeveldbIterator itr = null;
		private LevelQueue queue = null;
		
		LevelQueueIterator(LevelQueue q, LeveldbIterator i, Object key) {
			if (key != null) {
				JniBuffer buf = null;
				try {
					byte[] bkey = null;
					if(key instanceof byte[]) {
						bkey = (byte[])key;
					} else if(key instanceof String) {
						bkey = Time12SequenceId.toBinary((String)key);
					}
					if(bkey == null || bkey.length != Time12SequenceId.ID_LENGTH) {
						throw new LeveldbException("指定されたシーケンスIDの解釈に失敗しました.");
					}
					buf = LevelBuffer.key(LevelOption.TYPE_FREE, bkey);
					bkey = null;
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
//		protected void finalize() throws Exception {
//			close();
//		}

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
		}

		@Override
		public boolean isClose() {
			return itr == null || itr.isClose();
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
		public Object next() {
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
				this.resultKey = Time12SequenceId.toString(keyBuf.getBinary());
				Object value = LevelValues.decode(valBuf);
				LevelBuffer.clearBuffer(keyBuf, valBuf);
				keyBuf = null; valBuf = null;
				return value;
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
