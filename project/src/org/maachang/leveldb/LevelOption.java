package org.maachang.leveldb;

import java.util.Map;

import org.maachang.leveldb.util.Converter;

/**
 * LevelOptionオプション.
 */
public final class LevelOption {
	
	/** キー格納タイプ : なし. **/
	public static final int TYPE_NONE = -1;

	/** キー格納タイプ : 文字列. **/
	public static final int TYPE_STRING = 0;

	/** キー格納タイプ : 数字(32bit). **/
	public static final int TYPE_NUMBER32 = 1;

	/** キー格納タイプ : 数字(64bit). **/
	public static final int TYPE_NUMBER64 = 2;

	/** キー格納タイプ : ２キー[文字列]「文字列]. **/
	public static final int TYPE_STR_STR = 3;

	/** キー格納タイプ : ２キー[文字列]「数字(32bit)]. **/
	public static final int TYPE_STR_N32 = 4;

	/** キー格納タイプ : ２キー[文字列]「数字(64bit)]. **/
	public static final int TYPE_STR_N64 = 5;

	/** キー格納タイプ : ２キー[数字(32bit)]「文字列]. **/
	public static final int TYPE_N32_STR = 6;

	/** キー格納タイプ : ２キー[数字(32bit)]「数字(32bit)]. **/
	public static final int TYPE_N32_N32 = 7;

	/** キー格納タイプ : ２キー[数字(32bit)]「数字(64bit)]. **/
	public static final int TYPE_N32_N64 = 8;

	/** キー格納タイプ : ２キー[数字(64bit)]「文字列]. **/
	public static final int TYPE_N64_STR = 9;

	/** キー格納タイプ : ２キー[数字(64bit)]「数字(32bit)]. **/
	public static final int TYPE_N64_N32 = 10;

	/** キー格納タイプ : ２キー[数字(64bit)]「数字(64bit)]. **/
	public static final int TYPE_N64_N64 = 11;

	/** キー格納タイプ : ２キー[文字列]「バイナリ]. **/
	public static final int TYPE_STR_BIN = 12;

	/** キー格納タイプ : ２キー[数字(32bit)]「バイナリ]. **/
	public static final int TYPE_N32_BIN = 13;

	/** キー格納タイプ : ２キー[数字(64bit)]「バイナリ]. **/
	public static final int TYPE_N64_BIN = 14;

	/** キー格納タイプ : ２キー[バイナリ]「文字列]. **/
	public static final int TYPE_BIN_STR = 15;

	/** キー格納タイプ : ２キー[バイナリ]「数字(32bit)]. **/
	public static final int TYPE_BIN_N32 = 16;

	/** キー格納タイプ : ２キー[バイナリ]「数字(64bit)]. **/
	public static final int TYPE_BIN_N64 = 17;

	/** キー格納タイプ : ２キー[バイナリ]「バイナリ]. **/
	public static final int TYPE_BIN_BIN = 18;

	/** キー格納タイプ : マルチキー **/
	public static final int TYPE_MULTI = 19;

	/** キー格納タイプ : 自由定義(Binary). **/
	public static final int TYPE_FREE = 20;

	/**
	 * パラメータ長リスト. 0の場合は、マルチキー. 1の場合は単一キー. 2の場合は２キー.
	 */
	public static final int[] TYPE_PARAM_LENGTH = new int[] { 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			2, 0, 1 };

	/** block_cache最小値. **/
	private static final int MIN_BLOCK_SIZE = 8;

	/** オプション. **/
	protected int type = LevelOption.TYPE_STRING;
	protected int write_buffer_size = -1;
	protected int max_open_files = -1;
	protected int block_size = -1;
	protected int block_cache = -1;
	protected int block_restart_interval = -1;

	/**
	 * LevelOption生成.
	 * 
	 * @param args
	 *            オプションパラメータを設定します. [0]type. キータイプ. [1]write_buffer_size. 書き込みバッファ数.
	 *            [2]max_open_files. オープン最大ファイル数. [3]block_size. ブロックサイズ.
	 *            [4]block_cache. ブロックキャッシュ.
	 */
	public static final LevelOption create(Object... args) {
		return new LevelOption(args);
	}

	/**
	 * LevelOption生成.
	 * 
	 * @param buf
	 *            JniBufferを設定します.
	 */
	public static final LevelOption create(JniBuffer buf) {
		return new LevelOption(buf);
	}

	/**
	 * LevelOption生成.
	 * 
	 * @param args
	 *            オプションパラメータを設定します. args.get("type") キータイプ. args.get("bufferSize")
	 *            書き込みバッファサイズ. args.get("openFiles") オープン最大ファイル数.
	 *            args.get("blockSize") ブロックサイズ. args.get("blockCache") ブロックキャッシュ.
	 */
	public static final LevelOption create(Map<String, Object> args) {
		return new LevelOption(args);
	}

	/**
	 * コンストラクタ.
	 */
	public LevelOption() {
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param o
	 *            バッファポジションを設定します.
	 * @param buf
	 *            対象のJniBufferを設定します.
	 */
	public LevelOption(int[] o, JniBuffer buf) {
		long p = buf.address();
		type = LevelValues.byte4Int(p, o);
		write_buffer_size = LevelValues.byte4Int(p, o);
		max_open_files = LevelValues.byte4Int(p, o);
		block_size = LevelValues.byte4Int(p, o);
		block_cache = LevelValues.byte4Int(p, o);
		block_restart_interval = LevelValues.byte4Int(p, o);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param args
	 *            オプションパラメータを設定します. [0]type. キータイプ. [1]write_buffer_size. 書き込みバッファ数.
	 *            [2]max_open_files. オープン最大ファイル数. [3]block_size. ブロックサイズ.
	 *            [4]block_cache. ブロックキャッシュ. [5]block_restart_interval
	 */
	public LevelOption(Object... args) {
		if (args == null || args.length == 0) {
			return;
		}
		int len = args.length;
		if (len >= 1) {
			if (Converter.isNumeric(args[0])) {
				setType(Converter.convertInt(args[0]));
			} else {
				setType(Converter.convertString(args[0]));
			}
		}
		if (len >= 2 && Converter.isNumeric(args[1])) {
			setWriteBufferSize(Converter.convertInt(args[1]));
		}
		if (len >= 3 && Converter.isNumeric(args[2])) {
			setMaxOpenFiles(Converter.convertInt(args[2]));
		}
		if (len >= 4 && Converter.isNumeric(args[3])) {
			setBlockSize(Converter.convertInt(args[3]));
		}
		if (len >= 5 && Converter.isNumeric(args[4])) {
			setBlockCache(Converter.convertInt(args[4]));
		}
		if (len >= 6 && Converter.isNumeric(args[5])) {
			setBlockRestartInterval(Converter.convertInt(args[5]));
		}

	}

	/**
	 * コンストラクタ.
	 * 
	 * @param args
	 *            オプションパラメータを設定します. args.get("type") キータイプ. args.get("bufferSize")
	 *            書き込みバッファサイズ. args.get("openFiles") オープン最大ファイル数.
	 *            args.get("blockSize") ブロックサイズ. args.get("blockCache") ブロックキャッシュ.
	 *            args.get("blockRestartInterval");
	 */
	public LevelOption(Map<String, Object> args) {
		this(args.get("type"), args.get("bufferSize"), args.get("openFiles"), args.get("blockSize"),
				args.get("blockCache"), args.get("blockRestartInterval"));
	}

	/** タイプパターン定義. **/
	private static final String[] PATTERN_STR = new String[] { "str", "string", "char" };
	private static final String[] PATTERN_INT = new String[] { "n32", "int", "integer", "number32" };
	private static final String[] PATTERN_LONG = new String[] { "n64", "long", "number64", "bigint" };
	private static final String[] PATTERN_BINARY = new String[] { "binary", "bin" };
	private static final String[] PATTERN_MULTI = new String[] { "multi" };
	private static final String[] PATTERN_FREE = new String[] { "free" };

	// パターンチェック.
	private static final boolean pattern(int mode, String[] n, String c) {
		int len = n.length;
		// eq.
		if (mode == 0) {
			for (int i = 0; i < len; i++) {
				if (c.equals(n[i])) {
					return true;
				}
			}
		}
		// start.
		else if (mode == 1) {
			for (int i = 0; i < len; i++) {
				if (c.startsWith(n[i])) {
					return true;
				}
			}
		}
		// end.
		else if (mode == 2) {
			for (int i = 0; i < len; i++) {
				if (c.endsWith(n[i])) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * 文字列から、キータイプを取得.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @return int キータイプが返却されます.
	 */
	public static final int convertType(String value) {
		if (value == null || (value = value.trim().toLowerCase()).length() <= 0) {
			return LevelOption.TYPE_NONE;
		} else if("none".equals(value)) {
			return LevelOption.TYPE_NONE;
		} else if (pattern(0, PATTERN_STR, value)) {
			return LevelOption.TYPE_STRING;
		} else if (pattern(0, PATTERN_INT, value)) {
			return LevelOption.TYPE_NUMBER32;
		} else if (pattern(0, PATTERN_LONG, value)) {
			return LevelOption.TYPE_NUMBER64;
		} else if (pattern(1, PATTERN_STR, value) && pattern(2, PATTERN_STR, value)) {
			return LevelOption.TYPE_STR_STR;
		} else if (pattern(1, PATTERN_STR, value) && pattern(2, PATTERN_INT, value)) {
			return LevelOption.TYPE_STR_N32;
		} else if (pattern(1, PATTERN_STR, value) && pattern(2, PATTERN_LONG, value)) {
			return LevelOption.TYPE_STR_N64;
		} else if (pattern(1, PATTERN_INT, value) && pattern(2, PATTERN_STR, value)) {
			return LevelOption.TYPE_N32_STR;
		} else if (pattern(1, PATTERN_INT, value) && pattern(2, PATTERN_INT, value)) {
			return LevelOption.TYPE_N32_N32;
		} else if (pattern(1, PATTERN_INT, value) && pattern(2, PATTERN_LONG, value)) {
			return LevelOption.TYPE_N32_N64;
		} else if (pattern(1, PATTERN_LONG, value) && pattern(2, PATTERN_STR, value)) {
			return LevelOption.TYPE_N64_STR;
		} else if (pattern(1, PATTERN_LONG, value) && pattern(2, PATTERN_INT, value)) {
			return LevelOption.TYPE_N64_N32;
		} else if (pattern(1, PATTERN_LONG, value) && pattern(2, PATTERN_LONG, value)) {
			return LevelOption.TYPE_N64_N64;
		} else if (pattern(1, PATTERN_STR, value) && pattern(2, PATTERN_BINARY, value)) {
			return LevelOption.TYPE_STR_BIN;
		} else if (pattern(1, PATTERN_INT, value) && pattern(2, PATTERN_BINARY, value)) {
			return LevelOption.TYPE_N32_BIN;
		} else if (pattern(1, PATTERN_LONG, value) && pattern(2, PATTERN_BINARY, value)) {
			return LevelOption.TYPE_N64_BIN;
		} else if (pattern(1, PATTERN_BINARY, value) && pattern(2, PATTERN_STR, value)) {
			return LevelOption.TYPE_BIN_STR;
		} else if (pattern(1, PATTERN_BINARY, value) && pattern(2, PATTERN_INT, value)) {
			return LevelOption.TYPE_BIN_N32;
		} else if (pattern(1, PATTERN_BINARY, value) && pattern(2, PATTERN_LONG, value)) {
			return LevelOption.TYPE_BIN_N64;
		} else if (pattern(1, PATTERN_BINARY, value) && pattern(2, PATTERN_BINARY, value)) {
			return LevelOption.TYPE_BIN_BIN;
		} else if (pattern(1, PATTERN_MULTI, value)) {
			return LevelOption.TYPE_MULTI;
		} else if (pattern(1, PATTERN_BINARY, value) || pattern(0, PATTERN_FREE, value)) {
			return LevelOption.TYPE_FREE;
		}
		return LevelOption.TYPE_NONE;
	}

	/**
	 * 対象のキータイプを文字列変換.
	 * 
	 * @param type
	 *            対象のキータイプを設定します.
	 * @return String 文字列が返却されます.
	 */
	public static final String stringType(int type) {
		switch (type) {
		case LevelOption.TYPE_NONE:
			return "none";
		case LevelOption.TYPE_STRING:
			return "string";
		case LevelOption.TYPE_NUMBER32:
			return "number32";
		case LevelOption.TYPE_NUMBER64:
			return "number64";
		case LevelOption.TYPE_STR_STR:
			return "string-string";
		case LevelOption.TYPE_STR_N32:
			return "string-number32";
		case LevelOption.TYPE_STR_N64:
			return "string-number64";
		case LevelOption.TYPE_N32_STR:
			return "number32-string";
		case LevelOption.TYPE_N32_N32:
			return "number32-number32";
		case LevelOption.TYPE_N32_N64:
			return "number32-number64";
		case LevelOption.TYPE_N64_STR:
			return "number64-string";
		case LevelOption.TYPE_N64_N32:
			return "number64-number32";
		case LevelOption.TYPE_N64_N64:
			return "number64-number64";
		case LevelOption.TYPE_STR_BIN:
			return "string-binary";
		case LevelOption.TYPE_N32_BIN:
			return "number32-binary";
		case LevelOption.TYPE_N64_BIN:
			return "number64-binary";
		case LevelOption.TYPE_BIN_STR:
			return "binary-string";
		case LevelOption.TYPE_BIN_N32:
			return "binary-number32";
		case LevelOption.TYPE_BIN_N64:
			return "binary-number64";
		case LevelOption.TYPE_BIN_BIN:
			return "binary-binary";
		case LevelOption.TYPE_MULTI:
			return "multi";
		case LevelOption.TYPE_FREE:
			return "binary";
		default:
			return "none";
		}
	}
	
	/**
	 * ファーストキータイプを取得.
	 * 
	 * @param type
	 *            対象のキータイプを設定します.
	 * @return int キータイプが返却されます.
	 */
	public static final int getFirstKeyType(int type) {
		switch (type) {
		case LevelOption.TYPE_STRING:
			return LevelOption.TYPE_NONE;
		case LevelOption.TYPE_NUMBER32:
			return LevelOption.TYPE_NONE;
		case LevelOption.TYPE_NUMBER64:
			return LevelOption.TYPE_NONE;
		case LevelOption.TYPE_STR_STR:
			return LevelOption.TYPE_STRING;
		case LevelOption.TYPE_STR_N32:
			return LevelOption.TYPE_STRING;
		case LevelOption.TYPE_STR_N64:
			return LevelOption.TYPE_STRING;
		case LevelOption.TYPE_N32_STR:
			return LevelOption.TYPE_NUMBER32;
		case LevelOption.TYPE_N32_N32:
			return LevelOption.TYPE_NUMBER32;
		case LevelOption.TYPE_N32_N64:
			return LevelOption.TYPE_NUMBER32;
		case LevelOption.TYPE_N64_STR:
			return LevelOption.TYPE_NUMBER64;
		case LevelOption.TYPE_N64_N32:
			return LevelOption.TYPE_NUMBER64;
		case LevelOption.TYPE_N64_N64:
			return LevelOption.TYPE_NUMBER64;
		case LevelOption.TYPE_STR_BIN:
			return LevelOption.TYPE_STRING;
		case LevelOption.TYPE_N32_BIN:
			return LevelOption.TYPE_NUMBER32;
		case LevelOption.TYPE_N64_BIN:
			return LevelOption.TYPE_NUMBER64;
		case LevelOption.TYPE_BIN_STR:
			return LevelOption.TYPE_FREE;
		case LevelOption.TYPE_BIN_N32:
			return LevelOption.TYPE_FREE;
		case LevelOption.TYPE_BIN_N64:
			return LevelOption.TYPE_FREE;
		case LevelOption.TYPE_BIN_BIN:
			return LevelOption.TYPE_FREE;
		case LevelOption.TYPE_MULTI:
			return LevelOption.TYPE_NONE;
		case LevelOption.TYPE_FREE:
			return LevelOption.TYPE_NONE;
		default:
			return LevelOption.TYPE_NONE;
		}
	}
	
	/**
	 * セカンドキータイプを取得.
	 * 
	 * @param type
	 *            対象のキータイプを設定します.
	 * @return int キータイプが返却されます.
	 */
	public static final int getSecondKeyType(int type) {
		switch (type) {
		case LevelOption.TYPE_STRING:
			return LevelOption.TYPE_NONE;
		case LevelOption.TYPE_NUMBER32:
			return LevelOption.TYPE_NONE;
		case LevelOption.TYPE_NUMBER64:
			return LevelOption.TYPE_NONE;
		case LevelOption.TYPE_STR_STR:
			return LevelOption.TYPE_STRING;
		case LevelOption.TYPE_STR_N32:
			return LevelOption.TYPE_NUMBER32;
		case LevelOption.TYPE_STR_N64:
			return LevelOption.TYPE_NUMBER64;
		case LevelOption.TYPE_N32_STR:
			return LevelOption.TYPE_STRING;
		case LevelOption.TYPE_N32_N32:
			return LevelOption.TYPE_NUMBER32;
		case LevelOption.TYPE_N32_N64:
			return LevelOption.TYPE_NUMBER64;
		case LevelOption.TYPE_N64_STR:
			return LevelOption.TYPE_STRING;
		case LevelOption.TYPE_N64_N32:
			return LevelOption.TYPE_NUMBER32;
		case LevelOption.TYPE_N64_N64:
			return LevelOption.TYPE_NUMBER64;
		case LevelOption.TYPE_STR_BIN:
			return LevelOption.TYPE_FREE;
		case LevelOption.TYPE_N32_BIN:
			return LevelOption.TYPE_FREE;
		case LevelOption.TYPE_N64_BIN:
			return LevelOption.TYPE_FREE;
		case LevelOption.TYPE_BIN_STR:
			return LevelOption.TYPE_STRING;
		case LevelOption.TYPE_BIN_N32:
			return LevelOption.TYPE_NUMBER32;
		case LevelOption.TYPE_BIN_N64:
			return LevelOption.TYPE_NUMBER64;
		case LevelOption.TYPE_BIN_BIN:
			return LevelOption.TYPE_FREE;
		case LevelOption.TYPE_MULTI:
			return LevelOption.TYPE_NONE;
		case LevelOption.TYPE_FREE:
			return LevelOption.TYPE_NONE;
		default:
			return LevelOption.TYPE_NONE;
		}
	}
	
	/**
	 * 有効なキータイプかチェック.
	 * 
	 * @param type
	 *            キータイプを設定します.
	 * @return int
	 *            キータイプが当てはまらない場合は[-1]が返却されます.
	 */
	public static final int checkType(int type) {
		switch (type) {
		case LevelOption.TYPE_STRING:
			return LevelOption.TYPE_STRING;
		case LevelOption.TYPE_NUMBER32:
			return LevelOption.TYPE_NUMBER32;
		case LevelOption.TYPE_NUMBER64:
			return LevelOption.TYPE_NUMBER64;
		case LevelOption.TYPE_STR_STR:
			return LevelOption.TYPE_STR_STR;
		case LevelOption.TYPE_STR_N32:
			return LevelOption.TYPE_STR_N32;
		case LevelOption.TYPE_STR_N64:
			return LevelOption.TYPE_STR_N64;
		case LevelOption.TYPE_N32_STR:
			return LevelOption.TYPE_N32_STR;
		case LevelOption.TYPE_N32_N32:
			return LevelOption.TYPE_N32_N32;
		case LevelOption.TYPE_N32_N64:
			return LevelOption.TYPE_N32_N64;
		case LevelOption.TYPE_N64_STR:
			return LevelOption.TYPE_N64_STR;
		case LevelOption.TYPE_N64_N32:
			return LevelOption.TYPE_N64_N32;
		case LevelOption.TYPE_N64_N64:
			return LevelOption.TYPE_N64_N64;
		case LevelOption.TYPE_STR_BIN:
			return LevelOption.TYPE_STR_BIN;
		case LevelOption.TYPE_N32_BIN:
			return LevelOption.TYPE_N32_BIN;
		case LevelOption.TYPE_N64_BIN:
			return LevelOption.TYPE_N64_BIN;
		case LevelOption.TYPE_BIN_STR:
			return LevelOption.TYPE_BIN_STR;
		case LevelOption.TYPE_BIN_N32:
			return LevelOption.TYPE_BIN_N32;
		case LevelOption.TYPE_BIN_N64:
			return LevelOption.TYPE_BIN_N64;
		case LevelOption.TYPE_BIN_BIN:
			return LevelOption.TYPE_BIN_BIN;
		case LevelOption.TYPE_MULTI:
			return LevelOption.TYPE_MULTI;
		case LevelOption.TYPE_FREE:
			return LevelOption.TYPE_FREE;
		}
		return LevelOption.TYPE_NONE;
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
	 * キータイプを設定.
	 * 
	 * @param type
	 *            キータイプを設定します.
	 */
	public final void setType(int type) {
		type = checkType(type);
		this.type = type;
	}

	/**
	 * Leveldbに渡すキータイプを取得.
	 * 
	 * @param type
	 *            対象のタイプを設定します.
	 * @return int Leveldbに私キーオプションが返却されます.
	 */
	protected static final int getLeveldbKeyType(int type) {
		switch (type) {
		case LevelOption.TYPE_STRING:
			return LevelOption.TYPE_STRING;
		case LevelOption.TYPE_NUMBER32:
			return LevelOption.TYPE_NUMBER32;
		case LevelOption.TYPE_NUMBER64:
			return LevelOption.TYPE_NUMBER64;
		case LevelOption.TYPE_STR_STR:
			return LevelOption.TYPE_STR_STR;
		case LevelOption.TYPE_STR_N32:
			return LevelOption.TYPE_STR_N32;
		case LevelOption.TYPE_STR_N64:
			return LevelOption.TYPE_STR_N64;
		case LevelOption.TYPE_N32_STR:
			return LevelOption.TYPE_N32_STR;
		case LevelOption.TYPE_N32_N32:
			return LevelOption.TYPE_N32_N32;
		case LevelOption.TYPE_N32_N64:
			return LevelOption.TYPE_N32_N64;
		case LevelOption.TYPE_N64_STR:
			return LevelOption.TYPE_N64_STR;
		case LevelOption.TYPE_N64_N32:
			return LevelOption.TYPE_N64_N32;
		case LevelOption.TYPE_N64_N64:
			return LevelOption.TYPE_N64_N64;
		case LevelOption.TYPE_STR_BIN:
			return LevelOption.TYPE_STR_STR;
		case LevelOption.TYPE_N32_BIN:
			return LevelOption.TYPE_N32_STR;
		case LevelOption.TYPE_N64_BIN:
			return LevelOption.TYPE_N64_STR;
		case LevelOption.TYPE_BIN_STR:
			return LevelOption.TYPE_STR_STR;
		case LevelOption.TYPE_BIN_N32:
			return LevelOption.TYPE_STR_N32;
		case LevelOption.TYPE_BIN_N64:
			return LevelOption.TYPE_STR_N64;
		case LevelOption.TYPE_BIN_BIN:
			return LevelOption.TYPE_STR_STR;
		case LevelOption.TYPE_MULTI:
			return LevelOption.TYPE_STRING;
		case LevelOption.TYPE_FREE:
			return LevelOption.TYPE_STRING;
		}
		return LevelOption.TYPE_STRING;
	}
	
	/**
	 * タイプモードを取得.
	 * @param type
	 * @return 0: multiキー.
	 *         1: シングルキー.
	 *         2: ２キー.
	 *         -1: 不明.
	 */
	public static final int typeMode(int type) {
		if(type <= LevelOption.TYPE_NONE) {
			return LevelOption.TYPE_NONE;
		}
		return TYPE_PARAM_LENGTH[type];
	}

	/**
	 * キータイプを設定.
	 * 
	 * @param type
	 *            対象のタイプを文字列で設定します.
	 */
	public final void setType(String value) {
		type = convertType(value);
	}

	/**
	 * 書き込みバッファサイズを取得.
	 * 
	 * @return int 書き込みバッファサイズが返却されます. [-1]の場合は、デフォルト定義です. メガバイト単位で定義します.
	 */
	public final int getWriteBufferSize() {
		return write_buffer_size;
	}

	/**
	 * 書き込みバッファサイズを設定.
	 * 
	 * @param write_buffer_size
	 *            書き込みバッファサイズを設定します. [-1]の場合は、デフォルト定義です. メガバイト単位で定義します.
	 */
	public final void setWriteBufferSize(int write_buffer_size) {
		if (write_buffer_size <= 0) {
			write_buffer_size = -1;
		}
		this.write_buffer_size = write_buffer_size;
	}

	/**
	 * 同時ファイルオープン数を取得.
	 * 
	 * @return int 同時ファイルオープン数が返却されます. [-1]の場合は、デフォルト定義です.
	 */
	public final int getMaxOpenFiles() {
		return max_open_files;
	}

	/**
	 * 同時ファイルオープン数を設定.
	 * 
	 * @param max_open_files
	 *            同時ファイルオープン数を設定します. [-1]の場合は、デフォルト定義です.
	 */
	public final void setMaxOpenFiles(int max_open_files) {
		if (max_open_files <= 0) {
			max_open_files = -1;
		}
		this.max_open_files = max_open_files;
	}

	/**
	 * ブロックサイズを取得.
	 * 
	 * @return int ブロックサイズが返却されます. [-1]の場合は、デフォルト定義です. キロバイト単位で定義します.
	 */
	public final int getBlockSize() {
		return block_size;
	}

	/**
	 * ブロックサイズを設定.
	 * 
	 * @param block_size
	 *            ブロックサイズを設定します. [-1]の場合は、デフォルト定義です. キロバイト単位で定義します.
	 */
	public final void setBlockSize(int block_size) {
		if (block_size <= 0) {
			block_size = -1;
		}
		this.block_size = block_size;
	}

	/**
	 * ブロックキャッシュサイズを取得.
	 * 
	 * @return int ブロックキャッシュサイズが返却されます. [-1]の場合は、デフォルト定義です. メガバイト単位で定義します.
	 */
	public final int getBlockCache() {
		return block_cache;
	}

	/**
	 * ブロックキャッシュサイズを設定.
	 * 
	 * @param block_cache
	 *            ブロックキャッシュサイズを設定します. [-1]の場合は、デフォルト定義です. メガバイト単位で定義します.
	 */
	public final void setBlockCache(int block_cache) {
		if (MIN_BLOCK_SIZE > block_cache) {
			block_cache = -1;
		}
		this.block_cache = block_cache;
	}

	public int getBlockRestartInterval() {
		return block_restart_interval;
	}

	public void setBlockRestartInterval(int block_restart_interval) {
		if (block_restart_interval <= 0) {
			block_restart_interval = -1;
		}
		this.block_restart_interval = block_restart_interval;
	}

	/**
	 * 文字列変換.
	 * 
	 * @return String 文字列が返却されます.
	 */
	public final String toString() {
		return new StringBuilder().append(" type:").append(stringType(type)).append(" write_buffer_size:")
				.append(write_buffer_size).append(" max_open_files:").append(max_open_files).append(" block_size:")
				.append(block_size).append(" block_cache:").append(block_cache).append(" block_restart_interval:")
				.append(block_restart_interval).toString();
	}

	/**
	 * バッファ変換.
	 * 
	 * @param out
	 *            対象のバッファ情報を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public final void toBuffer(JniBuffer out) throws Exception {
		LevelValues.byte4(out, type);
		LevelValues.byte4(out, write_buffer_size);
		LevelValues.byte4(out, max_open_files);
		LevelValues.byte4(out, block_size);
		LevelValues.byte4(out, block_cache);
		LevelValues.byte4(out, block_restart_interval);
	}
}
