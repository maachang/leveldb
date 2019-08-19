package org.maachang.leveldb;

/**
 * Leveldb-Iterator.
 */
public class LeveldbIterator {
	protected LeveldbIterator() {
	}

	protected Leveldb parent;
	protected long addr;
	protected long snapShot;

	/**
	 * コンストラクタ.
	 * @param mode [true]の場合、スナップショット用のIteratorを生成します.
	 * @param p Leveldbオブジェクトを設定します.
	 */
	protected LeveldbIterator(boolean mode, Leveldb p) {
		if (p == null || p.isClose()) {
			throw new LeveldbException("対象のLeveldbは既にクローズされているか、無効です.");
		}
		parent = p;
		// Snapshot用のIteratorを作成する場合.
		if (mode) {
			snapShot = jni.leveldb_ss_create(p.addr);
			addr = jni.leveldb_ss_iterator(p.addr, snapShot);
		}
		// 通常のIteratorを作成する場合.
		else {
			snapShot = 0L;
			addr = jni.leveldb_iterator(p.addr);
		}
		// 先頭に移動.
		jni.leveldb_itr_first(addr);
	}

	/**
	 * デストラクタ.
	 */
	protected final void finalize() {
		close();
	}

	/**
	 * クローズ.
	 */
	public void close() {
		if (addr != 0L) {
			jni.leveldb_itr_delete(addr);
			addr = 0L;
		}
		if (snapShot != 0L) {
			jni.leveldb_ss_destroy(parent.addr, snapShot);
			snapShot = 0L;
		}
		parent = null;
	}

	/**
	 * クローズしているかチェック.
	 * @return boolean [true]の場合、クローズしています.
	 */
	public boolean isClose() {
		return parent.closeFlag || addr == 0L;
	}

	/** check. **/
	protected void check() {
		if (parent.closeFlag || addr == 0L) {
			throw new LeveldbException("既にクローズされています");
		}
	}

	/**
	 * カーソル位置を先頭に移動.
	 */
	public void first() {
		check();
		jni.leveldb_itr_first(addr);
	}

	/**
	 * カーソル位置を最後に移動.
	 */
	public void last() {
		check();
		jni.leveldb_itr_last(addr);
	}

	/**
	 * カーソル位置を指定条件の位置まで移動.
	 * @param key 検索対象のキーを設定します.
	 */
	public void seek(final JniBuffer key) {
		check();
		if (key == null || key.position() == 0) {
			throw new LeveldbException("キー情報が設定されていません");
		}
		jni.leveldb_itr_seek(addr, key.address(), key.position());
	}

	/**
	 * 次のカーソル位置に移動. ※通常のIteratorとは違い、開始位置が0番目(-1番目でない)ので、 単純にwhile( hasNext() )
	 * next() のような呼び出しができないので注意.
	 */
	public void next() {
		check();
		jni.leveldb_itr_next(addr);
	}

	/**
	 * 前のカーソル位置に移動.
	 */
	public void before() {
		check();
		jni.leveldb_itr_before(addr);
	}

	/**
	 * 現在位置の情報が存在するかチェック.
	 * @return boolean [true]の場合、存在します.
	 */
	public boolean valid() {
		check();
		return jni.leveldb_itr_valid(addr) == 1;
	}

	/**
	 * 指定位置のキー情報を取得.
	 * @param out 格納先のJniBufferを設定します.
	 * @return int サイズが返却されます.
	 */
	public int key(final JniBuffer out) {
		check();
		if (out == null) {
			return -1;
		}
		long[] n = new long[] { out.address() };
		int len = jni.leveldb_itr_key(addr, n, out.length());
		if (len <= 0) {
			return 0;
		// leveldb_getでバッファが拡張された場合.
		} else if (len > out.length()) {
			// バッファ内容を再セット.
			out.set(n[0], len, len);
		} else {
			// ポジジョンをセット.
			out.position(len);
		}
		return len;
	}

	/**
	 * 指定位置の要素情報を取得.
	 * @param out 格納先のJniBufferを設定します.
	 * @return int サイズが返却されます.
	 */
	public int value(final JniBuffer out) {
		check();
		if (out == null) {
			return -1;
		}
		long[] n = new long[] { out.address() };
		int len = jni.leveldb_itr_value(addr, n, out.length());
		if (len <= 0) {
			return 0;
		// leveldb_getでバッファが拡張された場合.
		} else if (len > out.length()) {
			// バッファ内容を再セット.
			out.set(n[0], len, len);
		} else {
			// ポジジョンをセット.
			out.position(len);
		}
		return len;
	}
}
