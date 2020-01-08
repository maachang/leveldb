package org.maachang.leveldb;

/**
 * Leveldbバッチ書き込み.
 */
public class WriteBatch {
	protected long addr = 0L;
	protected int count = 0;
	protected int deleteCount = 0;
	protected int putCount = 0;

	/**
	 * コンストラクタ.
	 */
	public WriteBatch() {
		addr = jni.leveldb_wb_create();
	}

	/**
	 * コンストラクタ. バッチ書き込みの数が多い場合は、このコンストラクタで あらかじめバッファ長を設定するほうが、高速に追加できます.
	 * 
	 * @param length
	 *            WriteBatchの格納用バッファ長を設定します.
	 */
	public WriteBatch(int length) {
		if (length <= 0) {
			addr = jni.leveldb_wb_create();
		} else {
			addr = jni.leveldb_wb_create_by_size(length);
		}
	}

	/**
	 * デストラクタ.
	 */
//	public void finalize() throws Exception {
//		close();
//	}

	/**
	 * バッチ書き込み情報のクローズ.
	 */
	public void close() {
		if (addr != 0L) {
			jni.leveldb_wb_destroy(addr);
			addr = 0L;
		}
		count = 0;
		deleteCount = 0;
		putCount = 0;
	}

	/**
	 * バッチ書き込みのクリア. バッチ書き込みの数が多い場合は、このクリア処理で あらかじめバッファ長を設定するほうが、高速に追加できます.
	 * 
	 * @param length
	 *            WriteBatchの格納用バッファ長を設定します.
	 */
	public void clear(int length) {
		if (addr != 0L) {
			if (length <= 0) {
				jni.leveldb_wb_clear(addr);
			} else {
				jni.leveldb_wb_clear_by_size(addr, length);
			}
		}
		count = 0;
		deleteCount = 0;
		putCount = 0;
	}

	/**
	 * バッチ書き込みのクリア.
	 */
	public void clear() {
		clear(0);
	}

	/**
	 * クローズしているかチェック.
	 * 
	 * @return boolean [true]の場合、クローズしています.
	 */
	public boolean isClose() {
		return addr == 0L;
	}

	/** check. **/
	protected void check() {
		if (addr == 0L) {
			throw new LeveldbException("既にクローズされています");
		}
	}

	/**
	 * 情報セット.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param value
	 *            対象の要素を設定します.
	 */
	public void put(JniBuffer key, JniBuffer value) {
		check();
		if (key == null || value == null || key.position() == 0 || value.position() == 0) {
			throw new LeveldbException("引数は不正です");
		}
		jni.leveldb_wb_put(addr, key.address(), key.position(), value.address(), value.position());
		count++;
		putCount++;
	}

	/**
	 * 情報セット.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param keyLen
	 *            対象のキー長を設定します.
	 * @param value
	 *            対象の要素を設定します.
	 * @param valueLen
	 *            対象の要素長を設定します.
	 */
	public void put(long key, int keyLen, long value, int valueLen) {
		check();
		jni.leveldb_wb_put(addr, key, keyLen, value, valueLen);
		count++;
		putCount++;
	}

	/**
	 * 情報削除.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 */
	public void remove(JniBuffer key) {
		check();
		if (key == null || key.position() == 0) {
			throw new LeveldbException("キー情報が設定されていません");
		}
		jni.leveldb_wb_remove(addr, key.address(), key.position());
		count++;
		deleteCount++;
	}

	/**
	 * WriteBatchを書き込み.
	 * 
	 * @param db
	 *            書き込み先のLeveldbオブジェクトを設定します.
	 */
	public void execute(Leveldb db) {
		check();
		if (db == null || db.isClose()) {
			throw new LeveldbException("書き込み先のLeveldbオブジェクトはクローズされているか無効です");
		}
		if (count == 0) {
			return;
		}
		if (jni.leveldb_wb_flush(db.addr, addr) == -1) {
			throw new LeveldbException("Leveldbに対して、WriteBatch書き込みに失敗しました");
		}
	}

	/**
	 * WriteBatchカーソルを取得.
	 * 
	 * @return WriteBatchCursor WriteBatchカーソルが返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public WriteBatchCursor getCursor() throws Exception {
		check();
		return new WriteBatchCursor(this);
	}
}
