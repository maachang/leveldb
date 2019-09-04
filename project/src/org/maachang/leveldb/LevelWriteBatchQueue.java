package org.maachang.leveldb;

import org.maachang.leveldb.util.Flag;

/**
 * WriteBatch対応のQueueオブジェクト.
 */
public class LevelWriteBatchQueue implements Queue {
	protected Leveldb leveldb;
	protected Time12SequenceId sequenceId;
	protected boolean sub = false;
	protected WriteBatch _batch;
	protected LeveldbIterator _snapShot;
	protected final Flag closeFlag = new Flag();

	/**
	 * コンストラクタ.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 */
	public LevelWriteBatchQueue(String name) {
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
	public LevelWriteBatchQueue(String name, LevelOption option) {
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
	public LevelWriteBatchQueue(int machineId, String name) {
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
	public LevelWriteBatchQueue(int machineId, String name, LevelOption option) {
		// keyタイプは free(Time12SequenceId).
		option.type = LevelOption.TYPE_FREE;
		this.leveldb = new Leveldb(name, option);
		this.sequenceId = new Time12SequenceId(machineId);
		this.sub = true;
		this._batch = null;
		this._snapShot = null;
		this.closeFlag.set(false);
	}
	
	/**
	 * コンストラクタ.
	 * 
	 * @param db
	 *            Leveldbを設定します.
	 */
	public LevelWriteBatchQueue(Leveldb db) {
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
	public LevelWriteBatchQueue(int machineId, Leveldb db) {
		if(db.getType() != LevelOption.TYPE_FREE) {
			throw new LeveldbException("The key type of the opened Leveldb is not TYPE_FREE.");
		}
		this.leveldb = db;
		this.sequenceId = new Time12SequenceId(machineId);
		this.sub = false;
		this._batch = null;
		this._snapShot = null;
		this.closeFlag.set(false);
	}
	
	/**
	 * コンストラクタ.
	 * 
	 * @param queue
	 *            LevelQueueを設定します.
	 */
	public LevelWriteBatchQueue(LevelQueue queue) {
		this.leveldb = queue.leveldb;
		this.sequenceId = queue.sequenceId;
		this.sub = false;
		this._batch = null;
		this._snapShot = null;
		this.closeFlag.set(false);
	}

	/**
	 * デストラクタ.
	 */
	protected void finalize() throws Exception {
		close();
	}
	
	/**
	 * オブジェクトクローズ.
	 */
	public void close() {
		if(!closeFlag.setToGetBefore(true)) {
			if (_batch != null) {
				_batch.close();
				_batch = null;
			}
			if (_snapShot != null) {
				_snapShot.close();
				_snapShot = null;
			}
			if (sub) {
				leveldb.close();
			}
			leveldb = null;
			sequenceId = null;
		}
	}

	/** チェック処理. **/
	private void checkClose() {
		if (closeFlag.get()) {
			throw new LeveldbException("The object has already been cleared.");
		}
	}

	/** バッチ情報を作成. **/
	private WriteBatch writeBatch() {
		if (_batch == null) {
			_batch = new WriteBatch();
		}
		return _batch;
	}

	/** Snapshotを作成. **/
	private LeveldbIterator getSnapshot() {
		if (_snapShot == null) {
			_snapShot = leveldb.snapShot();
		}
		return _snapShot;
	}

	/**
	 * WriteBatchオブジェクトを取得.
	 * 
	 * @return WriteBatch WriteBatchオブジェクトが返却されます.
	 */
	public WriteBatch getWriteBatch() {
		return writeBatch();
	}

	/**
	 * WriteBatch内容を反映.
	 * 
	 * @exception Exception
	 *                例外.
	 */
	public void commit() throws Exception {
		checkClose();
		// バッチ反映.
		if (_batch != null) {
			_batch.flush(leveldb);
			_batch.close();
			_batch = null;
		}
		// スナップショットをクリア.
		if (_snapShot != null) {
			_snapShot.close();
			_snapShot = null;
		}
	}

	/**
	 * WriteBatch内容を破棄.
	 * 
	 * @exception Exception
	 *                例外.
	 */
	public void rollback() throws Exception {
		checkClose();
		// バッチクリア.
		if (_batch != null) {
			_batch.close();
			_batch = null;
		}
		// スナップショットをクリア.
		if (_snapShot != null) {
			_snapShot.close();
			_snapShot = null;
		}
	}

	/**
	 * クローズしているかチェック.
	 * 
	 * @return boolean [true]の場合、クローズしています.
	 */
	public boolean isClose() {
		return closeFlag.get();
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
	 * Leveldbのオプションを取得.
	 * 
	 * @return LevelOption オプションが返却されます.
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
			WriteBatch b = writeBatch();
			if (o instanceof JniBuffer) {
				b.put(keyBuf, (JniBuffer) o);
			} else {
				valBuf = LevelBuffer.value(o);
				b.put(keyBuf, valBuf);
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
		LeveldbIterator itr = null;
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
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
				return LevelValues.decode(valBuf);
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
	 * 情報が空かチェック.
	 * 
	 * @return boolean [true]の場合、空です.
	 */
	public boolean isEmpty() {
		checkClose();
		LeveldbIterator itr = getSnapshot();
		if (itr.valid()) {
			return false;
		}
		return true;
	}
	
	/**
	 * iteratorを取得.
	 * 
	 * @param key
	 * @return
	 */
	public QueueIterator iterator() {
		return new QueueIterator(this, getSnapshot(), null);
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
		return new QueueIterator(this, getSnapshot(), key);
	}
}
