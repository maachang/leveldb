package org.maachang.leveldb;

import org.maachang.leveldb.util.Flag;

/**
 * Levelキュー情報.
 */
public class LevelQueue implements Queue {
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
	 *            Leveldbを設定します.
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
	 *            Leveldbを設定します.
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
	 * Leveldbオブジェクトを取得.
	 * 
	 * @return Leveldb Leveldbオブジェクトが返却されます.
	 */
	public Leveldb getLeveldb() {
		checkClose();
		return leveldb;
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
			if(o instanceof JniBuffer) {
				leveldb.put(keyBuf, (JniBuffer)o);
			} else {
				valBuf = LevelBuffer.value(o);
				leveldb.put(keyBuf, valBuf);
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
				itr = null;
				return ret;
			}
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
		return leveldb.isEmpty();
	}

	/**
	 * iteratorを取得.
	 * 
	 * @param key
	 * @return
	 */
	public QueueIterator iterator() {
		return new QueueIterator(this, leveldb.snapShot(), null);
	}

	/**
	 * iteratorを取得.
	 * 
	 * @param time
	 *            開始位置のミリ秒からのunix時間を設定します.
	 * @return
	 */
	public QueueIterator iterator(long time) {
		return iterator(Time12SequenceId.createId(0, time, 0));
	}

	/**
	 * iteratorを取得.
	 * 
	 * @param key
	 *            開始位置のキー情報を設定します.
	 * @return
	 */
	public QueueIterator iterator(String key) {
		return iterator(Time12SequenceId.toBinary(key));
	}

	/**
	 * iteratorを取得.
	 * 
	 * @param key
	 *            開始位置のキー情報を設定します.
	 * @return
	 */
	public QueueIterator iterator(byte[] key) {
		Time12SequenceId.first(key);
		return new QueueIterator(this, leveldb.snapShot(), key);
	}
}
