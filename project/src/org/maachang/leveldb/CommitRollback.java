package org.maachang.leveldb;

import org.maachang.leveldb.util.Flag;

/**
 * コミットロールバック支援.
 */
abstract class CommitRollback {
	protected boolean sub = false;
	protected boolean writeBatchFlag = true;
	protected Leveldb leveldb;
	protected WriteBatch _batch;
	protected LeveldbIterator _snapShot;
	protected Flag closeFlag = new Flag();
	
	/**
	 * デストラクタ.
	 */
	protected void finalize() throws Exception {
		close();
	}
	
	/**
	 * 初期化処理.
	 * @param db leveldbオブジェクトを設定します.
	 * @param sub [true]を設定するとleveldbオブジェクトをcloseで処理します.
	 * @param writeBatchFlag commit/rollback処理を有効にする場合は[true].
	 */
	protected void init(Leveldb db, boolean sub, boolean writeBatchFlag) {
		this.leveldb = db;
		this.sub = sub; 
		this.writeBatchFlag = writeBatchFlag;
		this.closeFlag.set(false);
	}
	
	/**
	 * オブジェクトクローズ.
	 */
	public void close() {
		if(!closeFlag.setToGetBefore(true)) {
			if(writeBatchFlag) {
				if (_batch != null) {
					_batch.close();
					_batch = null;
				}
				if (_snapShot != null) {
					_snapShot.close();
					_snapShot = null;
				}
			}
			if (sub) {
				leveldb.close();
			}
			leveldb = null;
		}
	}

	// チェック処理.
	protected void checkClose() {
		if (closeFlag.get()) {
			throw new LeveldbException("The object has already been cleared.");
		}
	}

	// バッチ情報を作成.
	protected WriteBatch writeBatch() {
		if(writeBatchFlag) {
			if (_batch == null) {
				_batch = new WriteBatch();
			}
			return _batch;
		}
		return null;
	}

	// Snapshotを作成.
	protected LeveldbIterator getSnapshot() {
		if(writeBatchFlag) {
			if (_snapShot == null) {
				_snapShot = leveldb.snapShot();
			}
			return _snapShot;
		}
		return null;
	}
	
	// Leveldb内容をクリア.
	protected void clearLeveldb() {
		checkClose();
		JniBuffer key = null;
		try {
			// Iteratorで削除するので、超遅い.
			LeveldbIterator it = leveldb.iterator();
			key = LevelBuffer.key();
			while (it.valid()) {
				if (it.key(key) > 0) {
					leveldb.remove(key);
				}
				LevelBuffer.clearBuffer(key, null);
				it.next();
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(key, null);
		}
	}

	/**
	 * WriteBatchオブジェクトを取得.
	 * 
	 * @return WriteBatch WriteBatchオブジェクトが返却されます.
	 */
	public WriteBatch getWriteBatch() {
		if(writeBatchFlag) {
			return writeBatch();
		}
		return null;
	}

	/**
	 * WriteBatch内容を反映.
	 */
	public void commit() {
		checkClose();
		if(writeBatchFlag) {
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
	}

	/**
	 * WriteBatch内容を破棄.
	 */
	public void rollback() {
		checkClose();
		if(writeBatchFlag) {
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
	 * Leveldbキータイプを取得.
	 * 
	 * @return int キータイプが返却されます.
	 */
	public int getType() {
		checkClose();
		return leveldb.getType();
	}

	/**
	 * writeBatchモードかチェック.
	 * @return
	 */
	public boolean isWriteBatch() {
		return writeBatchFlag;
	}
}
