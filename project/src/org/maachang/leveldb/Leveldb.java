package org.maachang.leveldb;

import java.io.File;

/**
 * Leveldb.
 */
public final class Leveldb {
	protected volatile boolean closeFlag = true;
	protected long addr = 0L;
	protected String path;
	protected int type;
	protected LevelOption option;

	/**
	 * コンストラクタ.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public Leveldb(String path) throws Exception {
		this(path, null);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 * @exception Exception
	 *                例外.
	 */
	public Leveldb(String path, LevelOption option) throws Exception {
		if (path == null || (path = path.trim()).length() <= 0) {
			return;
		} else if (option == null) {
			option = new LevelOption();
		}
		long a = 0L;
		String s = new File(path).getCanonicalPath();
		JniBuffer b = new JniBuffer();
		b.setJniChar(s);
		try {
			a = jni.leveldb_open(b.address(), LevelOption.getLeveldbKeyType(option.type),
					option.write_buffer_size,
					option.max_open_files,
					option.block_size,
					option.block_restart_interval,
					option.block_cache);
		} finally {
			b.destroy();
		}
		if (a == 0L) {
			throw new LeveldbException("Leveldbのオープンに失敗:" + s);
		}
		this.addr = a;
		this.path = s;
		this.type = option.type;
		this.option = option;
		this.closeFlag = false;
	}

	/**
	 * デストラクタ.
	 */
	protected final void finalize() throws Exception {
		close();
	}

	/**
	 * クローズ.
	 */
	public synchronized final void close() {
		if (!closeFlag) {
			closeFlag = true;
			jni.leveldb_close(addr);
			addr = 0L;
		}
	}

	/**
	 * クローズしているかチェック.
	 * 
	 * @return boolean [true]の場合、クローズしています.
	 */
	public final boolean isClose() {
		return closeFlag;
	}

	/** check. **/
	protected final void check() {
		if (closeFlag) {
			throw new LeveldbException("既にクローズされています");
		}
	}

	/**
	 * オープンパス名を取得.
	 * 
	 * @return String オープンパス名が返却されます.
	 */
	public final String getPath() {
		return path;
	}

	/**
	 * キータイプを取得.
	 * 
	 * @return int キータイプが返却されます.
	 */
	public final int getType() {
		return type;
	}

	/**
	 * Leveldbのオプションを取得.
	 * 
	 * @return LevelOption オプションが返却されます.
	 */
	public final LevelOption getOption() {
		return option;
	}

	/**
	 * 情報セット.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param value
	 *            対象の要素を設定します.
	 */
	public final void put(final JniBuffer key, final JniBuffer value) {
		check();
		if (key == null || value == null || key.position() == 0 || value.position() == 0) {
			throw new LeveldbException("引数は不正です");
		} else if (jni.leveldb_put(addr, key.address(), key.position(), value.address(), value.position()) == -1) {
			throw new LeveldbException("書き込み処理は失敗しました");
		}
	}

	/**
	 * 情報取得.
	 * 
	 * @param out
	 *            取得用のJniBufferを設定します.
	 * @param key
	 *            対象のキーを設定します.
	 * @return int 取得されたデータ長が返却されます.
	 */
	public final int get(final JniBuffer out, final JniBuffer key) {
		check();
		if (key == null || key.position() == 0) {
			throw new LeveldbException("キー情報が設定されていません");
		}
		long[] n = new long[] { out.address() };
		int len = jni.leveldb_get(addr, key.address(), key.position(), n, out.length());
		if (len <= 0) {
			return 0;
		}
		// leveldb_getでバッファが拡張された場合.
		if (len > out.length()) {
			// バッファ内容を再セット.
			out.set(n[0], len, len);
		} else {
			// ポジジョンをセット.
			out.position(len);
		}
		return len;
	}

	/**
	 * 情報削除.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return boolean [true]の場合、削除されました.
	 */
	public final boolean remove(final JniBuffer key) {
		check();
		if (key == null || key.position() == 0) {
			throw new LeveldbException("キー情報が設定されていません");
		}
		return jni.leveldb_remove(addr, key.address(), key.position()) != -1;
	}

	/**
	 * 状態取得
	 * 
	 * @param out
	 *            取得用のJniBufferを設定します.
	 * @param cmd
	 *            対象のコマンドを設定します. [leveldb.num-files-at-level?]
	 *            このコマンドの後の?に番号をセットします. leveldb.stats ステータスが返却されます.
	 *            leveldb.sstables sstable情報が返却されます.
	 * @return int 取得されたデータ長が返却されます.
	 */
	public final int property(final JniBuffer out, final JniBuffer cmd) {
		check();
		if (out == null || cmd == null || cmd.position() == 0) {
			throw new LeveldbException("コマンド情報が設定されていません");
		}
		long[] n = new long[] { out.address() };
		int len = jni.leveldb_property(addr, cmd.address(), cmd.position(), n, out.length());
		if (len <= 0) {
			return 0;
		}
		// leveldb_getでバッファが拡張された場合.
		if (len > out.length()) {
			// バッファ内容を再セット.
			out.set(n[0], len, len);
		} else {
			// ポジジョンをセット.
			out.position(len);
		}
		return len;
	}

	/**
	 * Iteratorを取得.
	 * 
	 * @return LeveldbIterator Iteratorオブジェクトが返却されます.
	 */
	public final LeveldbIterator iterator() {
		return new LeveldbIterator(false, this);
	}

	/**
	 * SnapShotを取得.
	 * 
	 * @return LeveldbIterator Iteratorオブジェクトが返却されます.
	 */
	public final LeveldbIterator snapShot() {
		return new LeveldbIterator(true, this);
	}

	/**
	 * データベース情報の修復.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final void repair(String path) throws Exception {
		repair(path, null);
	}

	/**
	 * データベース情報の修復.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 * @exception Exception
	 *                例外.
	 */
	@SuppressWarnings("resource")
	public static final void repair(String path, LevelOption option)
		throws Exception {
		if (path == null || (path = path.trim()).length() <= 0) {
			return;
		} else if (option == null) {
			option = new LevelOption();
		}
		String s = new File(path).getCanonicalPath();
		JniBuffer b = new JniBuffer();
		try {
			b.setJniChar(s);
			jni.leveldb_repair(b.address(),
				LevelOption.getLeveldbKeyType(option.type),
				option.write_buffer_size,
				option.max_open_files,
				option.block_size,
				option.block_restart_interval);
		} finally {
			b.destroy();
		}
	}

	/**
	 * データベース情報の削除.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final void destroy(String path) throws Exception {
		destroy(path, null);
	}

	/**
	 * データベース情報の削除.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 * @exception Exception
	 *                例外.
	 */
	@SuppressWarnings("resource")
	public static final void destroy(String path, LevelOption option)
		throws Exception {
		if (path == null || (path = path.trim()).length() <= 0) {
			return;
		} else if (option == null) {
			option = new LevelOption();
		}
		String s = new File(path).getCanonicalPath();
		JniBuffer b = new JniBuffer();
		try {
			b.setJniChar(s);
			jni.leveldb_destroy(b.address(),
				LevelOption.getLeveldbKeyType(option.type),
				option.write_buffer_size,
				option.max_open_files,
				option.block_size,
				option.block_restart_interval);
		} finally {
			b.destroy();
		}
	}
}
