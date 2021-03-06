package org.maachang.leveldb;

import java.io.File;

import org.maachang.leveldb.util.Flag;

/**
 * Leveldb.
 */
public final class Leveldb {
	private static final long DEF_TIMEOUT = 1000L;
	private static final long MIN_TIMEOUT = 150L;
	private static final long MAX_TIMEOUT = 5000L;
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
		this(path, null, DEF_TIMEOUT);
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
		this(path, option, DEF_TIMEOUT);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param path
	 *            対象のファイル名を設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 * @param timeout
	 *            オープン失敗の場合のリトライタイムアウト値を設定します.
	 */
	@SuppressWarnings("resource")
	public Leveldb(String path, LevelOption option, long timeout) {
		if (path == null || (path = path.trim()).length() <= 0) {
			throw new LeveldbException("File name to open leveldb does not exist.");
		} else if (option == null) {
			option = new LevelOption();
		}
		if(timeout < MIN_TIMEOUT) {
			timeout = MIN_TIMEOUT;
		} else if(timeout > MAX_TIMEOUT) {
			timeout = MAX_TIMEOUT;
		}
		String s;
		long a = 0L;
		JniBuffer b;
		long timeoutTime = System.currentTimeMillis() + timeout;
		// タイムアウトまでリトライを行う.
		while(true) {
			s = null;
			a = 0L;
			b = null;
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
			// オープンに失敗した場合.
			if (a == 0L) {
				// タイムアウトを超えている場合はエラー返却.
				if(System.currentTimeMillis() > timeoutTime) {
					throw new LeveldbException("Failed to open Leveldb:" + s);
				}
				// 一定期間待機.
				try {
					Thread.sleep(30L);
				} catch(Exception e) {
					throw new LeveldbException(e);
				}
			// オープンに成功した場合.
			} else {
				break;
			}
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
//	protected final void finalize() throws Exception {
//		close();
//	}

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
		if (key == null || key.position() == 0) {
			throw new LeveldbException("Key information is not set.");
		} else if (value == null || value.position() == 0) {
			throw new LeveldbException("Value information is not set.");
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
		} else {
			out.setting(n, len);
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
		} else {
			out.setting(n, len);
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
	public final LeveldbIterator snapshot() {
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
	
	/**
	 * 指定キーで検索処理.
	 * @param reverse
	 * @param type
	 * @param lv
	 * @param key
	 * @param key2
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static final void search(LeveldbIterator lv, boolean reverse, int type, Object key, Object key2) {
		if(key == null) {
			return;
		}
		JniBuffer keyBuf = null;
		try {
			if(key instanceof JniBuffer) {
				keyBuf = (JniBuffer)key;
			} else {
				keyBuf = LevelBuffer.key(type, key, key2);
			}
			lv.seek(keyBuf);
			LevelBuffer.clearBuffer(keyBuf, null);
			if(lv.valid() && reverse) {
				// 逆カーソル移動の場合は、対象keyより大きな値の条件の手前まで移動.
				Comparable c, cc;
				c = (Comparable)LevelId.id(type, key, key2);
				while(lv.valid()) {
					lv.key(keyBuf);
					cc = (Comparable)LevelId.get(type, keyBuf);
					LevelBuffer.clearBuffer(keyBuf, null);
					if(c.compareTo(cc) < 0) {
						lv.before();
						break;
					}
					lv.next();
				}
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, null);
		}
	}
}
