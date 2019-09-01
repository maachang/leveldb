package org.maachang.leveldb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;

/**
 * jni.
 */
final class jni {
	private jni() {
	}

	/**
	 * Javaバージョン.
	 */
	public static final String VERSION = "0.0.1";

	/**
	 * ライブラリ名
	 */
	public static final String LIB_NAME = "leveldb";

	/**
	 * 32Bitバージョン.
	 */
	public static final String LIB_BIT_32 = "32-";

	/**
	 * 64Bitバージョン.
	 */
	public static final String LIB_BIT_64 = "64-";

	/**
	 * Windowsライブラリ拡張子.
	 */
	public static final String WINDOWS_LIB_PLUS = ".dll";

	/**
	 * Linuxライブラリ拡張子.
	 */
	public static final String LINUX_LIB_PLUS = ".so";

	/**
	 * macライブラリ拡張子.
	 */
	public static final String MAC_LIB_PLUS = ".dylib";

	/**
	 * Nativeライブラリパス.
	 */
	public static final String NATIVE_PACKAGE = "org/maachang/leveldb/native/";

	/**
	 * デフォルトライブラリ格納ディレクトリ名.
	 */
	public static final String DEFAULT_DIR = ".lib_work";

	/** 初期化フラグ. **/
	private static volatile boolean initFlag = false;

	static {
		// arm系の処理.
		String arm = System.getProperty("os.arch");
		if (arm.startsWith("arm")) {
			System.load(targetDynamincLib(true, new StringBuilder(LIB_NAME)
					.append("-arm-").append(
							VERSION).append(LINUX_LIB_PLUS).toString()));
			initFlag = true;
		}
		// intel系の処理.
		else {
			String name = LIB_NAME;
			StringBuilder libBuf = new StringBuilder().append(name).append("-");
			String osName = getOsName();
			int bit = getOsBit();
			if (bit == -1) {
				System.err.println("JavaVM条件の取得に失敗");
				System.exit(-1);
			}
			if ("windows".equals(osName) || "linux".equals(osName)) {
				if (bit == 32) {
					libBuf.append(LIB_BIT_32);
				} else if (bit == 64) {
					libBuf.append(LIB_BIT_64);
				}
			}
			String lib = libBuf.append(VERSION).toString();
			libBuf = null;
			if ("windows".equals(osName)) {
				lib += WINDOWS_LIB_PLUS;
				System.load(targetDynamincLib(true, lib));

				initFlag = true;
			} else if ("linux".equals(osName)) {
				lib += LINUX_LIB_PLUS;
				System.load(targetDynamincLib(true, lib));

				initFlag = true;
			} else if ("mac".equals(osName) && bit == 64) {
				lib += MAC_LIB_PLUS;
				System.load(targetDynamincLib(true, lib));

				initFlag = true;
			} else {
				System.err.println("NativeIO初期化処理失敗(unknown)");
				System.exit(-1);
			}
		}
	}

	/** ライブラリの読み込みができたかチェック. **/
	public static final boolean isInit() {
		return initFlag;
	}

	/** 文字列をNativeバイナリに変換. **/
	public static final byte[] nativeString(String s) {
		if (s == null || (s = s.trim()).length() <= 0) {
			return new byte[1];
		}
		byte[] b = s.getBytes();
		int len = b.length;
		byte[] ret = new byte[len + 1];
		System.arraycopy(b, 0, ret, 0, len);
		ret[len] = 0;
		return ret;
	}

	/** 対象OSの最適なライブラリを読み込む. **/
	public static final String targetDynamincLib(boolean mode, String lib) {
		String sp = System.getProperty("file.separator");
		File targetDir = null;
		targetDir = new File(new StringBuilder().append(
				System.getProperty("user.home")).append(sp).append(DEFAULT_DIR)
				.toString());

		if (targetDir.exists() == false) {
			targetDir.mkdirs();
		}
		File outFile = new File(targetDir, lib);
		if (mode) {
			String ntvDir = NATIVE_PACKAGE;
			ntvDir = ntvDir.trim();
			if (ntvDir.endsWith("/") == false) {
				ntvDir += "/";
			}
			if (ntvDir.startsWith("/")) {
				ntvDir = ntvDir.substring(1);
			}
			InputStream is = new BufferedInputStream(Thread.currentThread()
					.getContextClassLoader().getResourceAsStream(ntvDir + lib));
			if (isLibFile(outFile, is)) {
				try {
					OutputStream os = new BufferedOutputStream(
							new FileOutputStream(outFile));
					try {
						try {
							int n;
							byte[] b = new byte[4096];
							while (true) {
								if ((n = is.read(b)) <= 0) {
									break;
								}
								os.write(b, 0, n);
							}
							os.flush();
							os.close();
							os = null;
							is.close();
							is = null;
						} finally {
							if (is != null) {
								is.close();
								is = null;
							}
						}
					} finally {
						if (os != null) {
							os.close();
							os = null;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					outFile = null;
				}
			}
			if (is != null) {
				try {
					is.close();
				} catch (Exception e) {
				}
				is = null;
			}
		}
		if (outFile != null) {
			return outFile.getAbsolutePath();
		}
		return null;
	}

	/** ライブラリファイルを読み込み、サイズをチェック. **/
	private static final boolean isLibFile(File f, InputStream in) {
		boolean ret = false;
		try {
			if (f.exists() == false) {
				ret = true;
			} else {
				int len = (int) f.length();
				ret = (len <= 0 || len != in.available());
			}
		} catch (Exception e) {
			ret = false;
		}
		return ret;
	}

	/** フルパスを取得. **/
	public static final String fullPath(String path) throws IOException {
		return new File(path).getCanonicalPath();
	}

	/** OS名を取得. **/
	private static final String getOsName() {
		String pathSp = System.getProperty("path.separator");
		String osName = System.getProperty("os.name").toLowerCase(Locale.US);

		if (pathSp.equals(";")) {
			return "windows";
		} else if (pathSp.equals(":")) {
			if (osName.indexOf("mac") > -1) {
				return "mac";
			}
			return "linux";
		}
		return "unknown";
	}

	/** OSのビット取得. **/
	private static final int getOsBit() {
		String os = System.getProperty("sun.arch.data.mode");
		if (os != null && (os = os.trim()).length() > 0) {
			if ("32".equals(os)) {
				return 32;
			} else if ("64".equals(os)) {
				return 64;
			}
		}
		os = System.getProperty("os.arch");
		if (os == null || (os = os.trim()).length() <= 0) {
			return -1;
		}
		if (os.endsWith("32")) {
			return 32;
		} else if (os.endsWith("64")) {
			return 64;
		}
		return 32;
	}

	// memory-i/o.
	public static native long malloc(int size);

	public static native long realloc(long address, int size);

	public static native void free(long address);

	public static native void memset(long address, byte code, int size);

	public static native void memcpy(long destAddr, long srcAddr, int size);

	public static native int memcmp(long aAddr, long bAddr, int len);

	public static native byte getByte(long address);

	public static native void putByte(long address, byte value);

	public static native void getBinary(long address, byte[] binary,
		int off, int len);

	public static native void putBinary(long address, byte[] binary,
		int off, int len);

	public static native void putChar(long address, char value);

	public static native char getChar(long address);

	public static native void putShort(long address, short value);

	public static native short getShort(long address);

	public static native void putInt(long address, int value);

	public static native int getInt(long address);

	public static native void putLong(long address, long value);

	public static native long getLong(long address);

	public static native int eq(long srcAddr, long destAddr, int len);

	// snappy.
	public static native int snappyMaxCompressedLength(int oneCompressLength);

	public static native int snappyCompress(long src, int src_len, long dst,
			int[] dst_len);

	public static native int snappyDecompress(long src, int src_len,
			long dst, int[] dst_len);
	
	// lz4.
	public static native int lz4MaxCompressedLength(int oneCompressLength);

	public static native int lz4UncompressLength(long src);

	public static native int lz4Compress(long src, int src_len, long dst,
			int[] dst_len);

	public static native int lz4Decompress(long src, int src_len,
			long dst, int[] dst_len);

	// leveldb.
	public static native void leveldb_destroy(long name, int type,
			int write_buffer_size, int max_open_files, int block_size,
			int block_restart_interval);

	public static native void leveldb_repair(long name, int type,
			int write_buffer_size, int max_open_files, int block_size,
			int block_restart_interval);

	public static native long leveldb_open(long name, int type,
			int write_buffer_size, int max_open_files, int block_size,
			int block_restart_interval, int block_cache);

	// leveldb-i/o.
	public static native void leveldb_close(long db);

	public static native int leveldb_put(long db, long key, int kLen,
			long value, int vLen);

	public static native int leveldb_get(long db, long key, int len,
			long[] buf, int bufLen);

	public static native int leveldb_remove(long db, long key, int len);

	public static native long leveldb_iterator(long db);

	public static native int leveldb_property(long db, long cmd, int cmdLen,
			long[] buf, int bufLen);

	public static native int leveldb_vacuum(long db, long start,
			int startLen, long end, int endLen);

	// leveldb-iterator.
	public static native void leveldb_itr_delete(long itr); // iterator close.

	public static native void leveldb_itr_first(long itr);

	public static native void leveldb_itr_last(long itr);

	public static native void leveldb_itr_seek(long itr, long key, int len);

	public static native int leveldb_itr_valid(long itr);

	public static native void leveldb_itr_next(long itr);

	public static native void leveldb_itr_before(long itr);

	public static native int leveldb_itr_key(long itr, long[] buf, int bufLen);

	public static native int leveldb_itr_value(long itr, long[] buf,
			int bufLen);

	// Write-Batch.
	public static native long leveldb_wb_create();

	public static native long leveldb_wb_create_by_size(int len);

	public static native void leveldb_wb_destroy(long wb);

	public static native void leveldb_wb_clear_by_size(long wb, int len);

	public static native void leveldb_wb_clear(long wb);

	public static native void leveldb_wb_put(long wb, long key, int kLen,
			long value, int vLen);

	public static native void leveldb_wb_remove(long wb, long key, int len);

	public static native long leveldb_wb_values(long wb);

	public static native int leveldb_wb_values_size(long wb);

	public static native int leveldb_wb_flush(long db, long wb);

	// SnapShort.
	public static native long leveldb_ss_create(long db);

	public static native void leveldb_ss_destroy(long db, long ss);

	public static native long leveldb_ss_iterator(long db, long ss);
}
