package org.maachang.leveldb.operator;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.Leveldb;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.LeveldbIterator;
import org.maachang.leveldb.WriteBatch;
import org.maachang.leveldb.util.Flag;

/**
 * LeveldbOperator.
 */
public abstract class LevelOperator {
	/** オペレータタイプ: インデックス. **/
	public static final int LEVEL_INDEX = 1;
	
	/** オペレータタイプ: Map. **/
	public static final int LEVEL_MAP = 2;
	
	/** オペレータタイプ: キュー. **/
	public static final int LEVEL_QUEUE = 3;
	
	/** オペレータタイプ: シーケンス. **/
	public static final int LEVEL_SEQUENCE = 4;
	
	/** オペレータタイプ: 緯度経度. **/
	public static final int LEVEL_LAT_LON = 5;
	
	protected boolean sub = false;
	protected boolean writeBatchFlag = true;
	protected Leveldb leveldb;
	protected WriteBatch _batch;
	protected LeveldbIterator _snapshot;
	protected Flag parentCloseFlag = null;
	protected Flag closeFlag = new Flag();
	
	// rwlock.
	// このオブジェクトはこの上位で呼び出して利用する
	// ためのものなので、この情報は基本利用しない.
	protected final ReadWriteLock __rwLock = new ReentrantReadWriteLock();
	
	/**
	 * デストラクタ.
	 */
//	protected void finalize() throws Exception {
//		close();
//	}
	
	/**
	 * 初期化処理.
	 * @param opr 元のオブジェクトを設定します.
	 * @param db leveldbオブジェクトを設定します.
	 * @param sub [true]を設定するとleveldbオブジェクトをcloseで処理します.
	 * @param writeBatchFlag commit/rollback処理を有効にする場合は[true].
	 */
	protected void init(LevelOperator opr, Leveldb db, boolean sub, boolean writeBatchFlag) {
		this.leveldb = db;
		this.sub = sub; 
		this.writeBatchFlag = writeBatchFlag;
		if(opr != null) {
			parentCloseFlag = opr.closeFlag;
		} else {
			parentCloseFlag = new Flag(false);
		}
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
				if (_snapshot != null) {
					_snapshot.close();
					_snapshot = null;
				}
			}
			if (sub) {
				if(leveldb != null && !parentCloseFlag.get()) {
					leveldb.close();
				}
			}
			leveldb = null;
		}
	}
	
	// チェック処理.
	protected void checkClose() {
		if (parentCloseFlag.get() || closeFlag.get()) {
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
			if (_snapshot == null) {
				_snapshot = leveldb.snapshot();
			}
			return _snapshot;
		}
		return null;
	}
	
	/**
	 * このオペレータを完全破棄.
	 * @return boolean [true]の場合、削除成功.
	 */
	public boolean deleteComplete() {
		checkClose();
		if(writeBatchFlag) {
			return false;
		}
		if(!closeFlag.setToGetBefore(true)) {
			String path = leveldb.getPath();
			LevelOption opt = leveldb.getOption().copyObject();
			leveldb.close();
			leveldb = null;
			Leveldb.destroy(path, opt);
			return true;
		}
		return false;
	}
	
	/**
	 * このオペレータのデータを完全削除.
	 * @return boolean [true]の場合、データの完全削除が成功しました.
	 */
	public boolean trancate() {
		checkClose();
		if(writeBatchFlag) {
			return false;
		}
		String path = leveldb.getPath();
		LevelOption opt = leveldb.getOption().copyObject();
		leveldb.close();
		leveldb = null;
		Leveldb.destroy(path, opt);
		leveldb = new Leveldb(path, opt);
		return true;
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
				_batch.execute(leveldb);
				_batch.close();
				_batch = null;
			}
			// スナップショットをクリア.
			if (_snapshot != null) {
				_snapshot.close();
				_snapshot = null;
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
			if (_snapshot != null) {
				_snapshot.close();
				_snapshot = null;
			}
		}
	}

	/**
	 * クローズしているかチェック.
	 * 
	 * @return boolean [true]の場合、クローズしています.
	 */
	public boolean isClose() {
		return parentCloseFlag.get() || closeFlag.get();
	}
	
	/**
	 * 親オペレータがクローズしているかチェック.
	 * 
	 * @return boolean [true]の場合、クローズしています.
	 */
	public boolean isParentClose() {
		return parentCloseFlag.get();
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
	
	/**
	 * ReadWriteLockオブジェクトを取得.
	 * @return
	 */
	public ReadWriteLock getLock() {
		return __rwLock;
	}
	
	/**
	 * オペレータタイプ.
	 * @return int オペレータタイプが返却されます.
	 */
	public abstract int getOperatorType();
}
