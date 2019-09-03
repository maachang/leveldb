package org.maachang.leveldb;

import java.io.File;

import org.maachang.leveldb.util.Flag;

/**
 * Leveldb.
 */
public final class Leveldb {
	protected long addr = 0L;
	protected String path;
	protected int type;
	protected LevelOption option;
	
	protected final Flag closeFlag = new Flag();

	/**
	 * コンストラクタ.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 */
	public Leveldb(String path) {
		this(path, null);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 */
	@SuppressWarnings("resource")
	public Leveldb(String path, LevelOption option) {
		if (path == null || (path = path.trim()).length() <= 0) {
			throw new LeveldbException("File name to open leveldb does not exist.");
		} else if (option == null) {
			option = new LevelOption();
		}
		String s;
		long a = 0L;
		JniBuffer b = null;
		try {
			s = new File(path).getCanonicalPath();
			b = new JniBuffer();
			b.setJniChar(s);
			a = jni.leveldb_open(b.address(), LevelOption.getLeveldbKeyType(option.type), option.write_buffer_size,
					option.max_open_files, option.block_size, option.block_restart_interval, option.block_cache);
			b.destroy();
			b = null;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (b != null) {
				b.destroy();
			}
		}
		if (a == 0L) {
			throw new LeveldbException("Failed to open Leveldb:" + s);
		}
		this.addr = a;
		this.path = s;
		this.type = option.type;
		this.option = option;
		this.closeFlag.set(false);
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
	public final void close() {
		if (!closeFlag.setToGetBefore(true)) {
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
		return closeFlag.get();
	}

	// クローズチェック.
	protected final void checkClose() {
		if (closeFlag.get()) {
			throw new LeveldbException("Already closed.");
		}
	}

	/**
	 * オープンパス名を取得.
	 * 
	 * @return String オープンパス名が返却されます.
	 */
	public final String getPath() {
		checkClose();
		return path;
	}

	/**
	 * キータイプを取得.
	 * 
	 * @return int キータイプが返却されます.
	 */
	public final int getType() {
		checkClose();
		return type;
	}

	/**
	 * Leveldbのオプションを取得.
	 * 
	 * @return LevelOption オプションが返却されます.
	 */
	public final LevelOption getOption() {
		checkClose();
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
		checkClose();
		if (key == null || value == null || key.position() == 0 || value.position() == 0) {
			throw new LeveldbException("Argument is invalid.");
		} else if (jni.leveldb_put(addr, key.address(), key.position(), value.address(), value.position()) == -1) {
			throw new LeveldbException("Put processing failed.");
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
		checkClose();
		if (key == null || key.position() == 0) {
			throw new LeveldbException("Key information is not set.");
		}
		final long[] n = new long[] { out.address() };
		final int len = jni.leveldb_get(addr, key.address(), key.position(), n, out.length());
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
		checkClose();
		if (key == null || key.position() == 0) {
			throw new LeveldbException("Key information is not set.");
		}
		return jni.leveldb_remove(addr, key.address(), key.position()) != -1;
	}

	/**
	 * 状態取得
	 * 
	 * @param out
	 *            取得用のJniBufferを設定します.
	 * @param cmd
	 *            対象のコマンドを設定します. [leveldb.num-files-at-level?] このコマンドの後の?に番号をセットします.
	 *            leveldb.stats ステータスが返却されます. leveldb.sstables sstable情報が返却されます.
	 * @return int 取得されたデータ長が返却されます.
	 */
	public final int property(final JniBuffer out, final JniBuffer cmd) {
		checkClose();
		if (out == null || cmd == null || cmd.position() == 0) {
			throw new LeveldbException("Command information is not set.");
		}
		final long[] n = new long[] { out.address() };
		final int len = jni.leveldb_property(addr, cmd.address(), cmd.position(), n, out.length());
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
		checkClose();
		return new LeveldbIterator(false, this);
	}

	/**
	 * SnapShotを取得.
	 * 
	 * @return LeveldbIterator Iteratorオブジェクトが返却されます.
	 */
	public final LeveldbIterator snapShot() {
		checkClose();
		return new LeveldbIterator(true, this);
	}

	/**
	 * 情報が空かチェック.
	 * 
	 * @return boolean [true]の場合、空です.
	 */
	public boolean isEmpty() {
		checkClose();
		LeveldbIterator itr = null;
		try {
			itr = iterator();
			boolean ret = !itr.valid();
			itr.close();
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
		}
	}

	/**
	 * データベース情報の修復.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 */
	public static final void repair(String path) {
		repair(path, null);
	}

	/**
	 * データベース情報の修復.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 */
	@SuppressWarnings("resource")
	public static final void repair(String path, LevelOption option) {
		if (path == null || (path = path.trim()).length() <= 0) {
			return;
		} else if (option == null) {
			option = new LevelOption();
		}
		JniBuffer b = null;
		try {
			String s = new File(path).getCanonicalPath();
			b = new JniBuffer();
			b.setJniChar(s);
			jni.leveldb_repair(b.address(), LevelOption.getLeveldbKeyType(option.type), option.write_buffer_size,
					option.max_open_files, option.block_size, option.block_restart_interval);
			b.destroy();
			b = null;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (b != null) {
				b.destroy();
			}
		}
	}

	/**
	 * データベース情報の削除.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 */
	public static final void destroy(String path) {
		destroy(path, null);
	}

	/**
	 * データベース情報の削除.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 */
	@SuppressWarnings("resource")
	public static final void destroy(String path, LevelOption option) {
		if (path == null || (path = path.trim()).length() <= 0) {
			return;
		} else if (option == null) {
			option = new LevelOption();
		}
		JniBuffer b = null;
		try {
			String s = new File(path).getCanonicalPath();
			b = new JniBuffer();
			b.setJniChar(s);
			jni.leveldb_destroy(b.address(), LevelOption.getLeveldbKeyType(option.type), option.write_buffer_size,
					option.max_open_files, option.block_size, option.block_restart_interval);
			b.destroy();
			b = null;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if (b != null) {
				b.destroy();
			}
		}
	}
}
