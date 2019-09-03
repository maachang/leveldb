package org.maachang.leveldb;

import java.nio.ByteBuffer;

/**
 * JNIメモリ操作.
 */
public final class JniIO {
	protected JniIO() {
	}

	/** sun.misc.Unsafeオブジェクト利用 **/
	private static final boolean UnsafeMode = Unsafe.UNSAFE_MODE;
	private static final sun.misc.Unsafe unsafe = Unsafe.get();

	/**
	 * メモリ生成.
	 * 
	 * @param len
	 *            生成メモリ長を設定します.
	 * @return long 先頭アドレスが返却されます.
	 */
	public static final long malloc(final int len) {
		return jni.malloc(len);
	}

	/**
	 * メモリ再生成.
	 * 
	 * @param addr
	 *            メモリアドレスを設定します.
	 * @param len
	 *            生成するメモリ長を設定します.
	 * @return long 先頭アドレスが返却されます.
	 */
	public static final long realloc(final long addr, final int len) {
		return jni.realloc(addr, len);
	}

	/**
	 * メモリ開放.
	 * 
	 * @param addr
	 *            メモリアドレスを設定します.
	 */
	public static final void free(final long addr) {
		jni.free(addr);
	}

	/**
	 * memset.
	 * 
	 * @param addr
	 *            メモリセットをするアドレスを設定します.
	 * @param code
	 *            セットする１バイト情報を設定します.
	 * @param len
	 *            セットする長さを設定します.
	 */
	public static final void memset(final long addr, final int code, final int len) {
		jni.memset(addr, (byte) code, len);
	}

	/**
	 * memset.
	 * 
	 * @param addr
	 *            メモリセットをするアドレスを設定します.
	 * @param code
	 *            セットする１バイト情報を設定します.
	 * @param len
	 *            セットする長さを設定します.
	 */
	public static final void memset(final long addr, final byte code, final int len) {
		jni.memset(addr, code, len);
	}

	/**
	 * memcpy.
	 * 
	 * @param dest
	 *            コピー先のアドレスを設定します.
	 * @param src
	 *            コピー元のアドレスを設定します.
	 * @param len
	 *            セットする長さを設定します.
	 */
	public static final void memcpy(final long dest, final long src, final int len) {
		jni.memcpy(dest, src, len);
	}

	/**
	 * JniBufferの内容が一致するかチェック.
	 * 
	 * @param a
	 *            比較元のアドレスを設定します.
	 * @param aLen
	 *            比較元の長さを設定します.
	 * @param b
	 *            比較先のアドレスを設定します.
	 * @param bLen
	 *            比較先の長さを設定します.
	 * @return boolean [true]の場合、一致しています.
	 */
	public static final boolean equals(long a, int aLen, long b, int bLen) {
		if (aLen == bLen) {

			// 長さが同じ場合、memcmpでチェック.
			return jni.memcmp(a, b, aLen) == 0;
		}
		return false;
	}

	/**
	 * 1バイトの情報を取得.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return byte バイト情報が返されます.
	 */
	public static final byte get(final long address, final int index) {
		if (UnsafeMode) {
			return unsafe.getByte(address + index);
		} else {
			return jni.getByte(address + index);
		}
	}

	/**
	 * 1バイトの情報を設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            対象の１バイト情報を設定します.
	 */
	public static final void put(final long address, final int index, final byte value) {
		if (UnsafeMode) {
			unsafe.putByte(address + index, value);
		} else {
			jni.putByte(address + index, value);
		}
	}

	/**
	 * binary情報を設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            設定対象の情報を設定します.
	 * @param offset
	 *            対象のオフセット値を設定します.
	 * @param length
	 *            対象のデータ長を設定します.
	 * @return int 設定された長さが返されます.
	 */
	public static final int putBinary(final long address, final int index, final byte[] value, final int offset,
			final int length) {
		jni.putBinary(address + index, value, offset, length);
		return length;
	}

	/**
	 * binary情報を取得.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            取得対象の情報を設定します.
	 * @param offset
	 *            対象のオフセット値を設定します.
	 * @param length
	 *            対象のデータ長を設定します.
	 * @return int 設定された長さが返されます.
	 */
	public static final int getBinary(final long address, final int index, final byte[] value, final int offset,
			final int length) {
		jni.getBinary(address + index, value, offset, length);
		return length;
	}

	/**
	 * boolean設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            設定対象の情報を設定します.
	 */
	public static final void putBoolean(final long address, final int index, final boolean value) {
		if (UnsafeMode) {
			unsafe.putByte(address + index, ((value) ? (byte) 1 : (byte) 0));
		} else {
			jni.putByte(address + index, ((value) ? (byte) 1 : (byte) 0));
		}
	}

	/**
	 * boolean取得.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return boolean 情報が返されます.
	 */
	public static final boolean getBoolean(final long address, final int index) {
		if (UnsafeMode) {
			return (unsafe.getByte(address + index) == 0) ? false : true;
		}
		return (jni.getByte(address + index) == 0) ? false : true;
	}

	/**
	 * char設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            設定対象の情報を設定します.
	 */
	public static final void putChar(final long address, final int index, char value) {
		if (UnsafeMode) {
			unsafe.putChar(address + index, value);
		} else {
			jni.putChar(address + index, value);
		}
	}

	/**
	 * char取得.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return char 情報が返されます.
	 */
	public static final char getChar(final long address, final int index) {
		if (UnsafeMode) {
			return unsafe.getChar(address + index);
		}
		return jni.getChar(address + index);
	}

	/**
	 * char取得.
	 * <p>
	 * Endian変換を行います.
	 * </p>
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return char 情報が返されます.
	 */
	public static final char getCharE(final long address, final int index) {
		if (Unsafe.BIG_ENDIAN) {
			return Unsafe.swap(getChar(address, index));
		}
		return getChar(address, index);
	}

	/**
	 * short設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            設定対象の情報を設定します.
	 */
	public static final void putShort(final long address, final int index, final short value) {
		if (UnsafeMode) {
			unsafe.putShort(address + index, value);
		} else {
			jni.putShort(address + index, value);
		}
	}

	/**
	 * short取得.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return short 情報が返されます.
	 */
	public static final short getShort(final long address, final int index) {
		if (UnsafeMode) {
			return unsafe.getShort(address + index);
		}
		return jni.getShort(address + index);
	}

	/**
	 * short取得.
	 * <p>
	 * Endian変換を行います.
	 * </p>
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return short 情報が返されます.
	 */
	public static final short getShortE(final long address, final int index) {
		if (Unsafe.BIG_ENDIAN) {
			return Unsafe.swap(getShort(address, index));
		}
		return getShort(address, index);
	}

	/**
	 * int設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            設定対象の情報を設定します.
	 */
	public static final void putInt(final long address, final int index, final int value) {
		if (UnsafeMode) {
			unsafe.putInt(address + index, value);
		} else {
			jni.putInt(address + index, value);
		}
	}

	/**
	 * int取得.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return int 情報が返されます.
	 */
	public static final int getInt(final long address, final int index) {
		if (UnsafeMode) {
			return unsafe.getInt(address + index);
		}
		return jni.getInt(address + index);
	}

	/**
	 * int取得.
	 * <p>
	 * Endian変換を行います.
	 * </p>
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return int 情報が返されます.
	 */
	public static final int getIntE(final long address, final int index) {
		if (Unsafe.BIG_ENDIAN) {
			return Unsafe.swap(getInt(address, index));
		}
		return getInt(address, index);
	}

	/**
	 * long設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            設定対象の情報を設定します.
	 */
	public static final void putLong(final long address, final int index, final long value) {
		if (UnsafeMode) {
			unsafe.putLong(address + index, value);
		} else {
			jni.putLong(address + index, value);
		}
	}

	/**
	 * long取得.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return long 情報が返されます.
	 */
	public static final long getLong(final long address, final int index) {
		if (UnsafeMode) {
			return unsafe.getLong(address + index);
		}
		return jni.getLong(address + index);
	}

	/**
	 * long取得.
	 * <p>
	 * Endian変換を行います.
	 * </p>
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return long 情報が返されます.
	 */
	public static final long getLongE(final long address, final int index) {
		if (Unsafe.BIG_ENDIAN) {
			return Unsafe.swap(getLong(address, index));
		}
		return getLong(address, index);
	}

	/**
	 * float設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            設定対象の情報を設定します.
	 */
	public static final void putFloat(final long address, final int index, final float value) {
		putInt(address, index, Float.floatToIntBits(value));
	}

	/**
	 * float取得.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return float 情報が返されます.
	 */
	public static final float getFloat(final long address, final int index) {
		return Float.intBitsToFloat(getInt(address, index));
	}

	/**
	 * float取得.
	 * <p>
	 * Endian変換を行います.
	 * </p>
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return float 情報が返されます.
	 */
	public static final float getFloatE(final long address, final int index) {
		if (Unsafe.BIG_ENDIAN) {
			return Float.intBitsToFloat(Unsafe.swap(getInt(address, index)));
		}
		return getFloat(address, index);
	}

	/**
	 * double設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            設定対象の情報を設定します.
	 */
	public static final void putDouble(final long address, final int index, final double value) {
		putLong(address, index, Double.doubleToLongBits(value));
	}

	/**
	 * double取得.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return double 情報が返されます.
	 */
	public static final double getDouble(final long address, final int index) {
		return Double.longBitsToDouble(getLong(address, index));
	}

	/**
	 * double取得.
	 * <p>
	 * Endian変換を行います.
	 * </p>
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @return double 情報が返されます.
	 */
	public static final double getDoubleE(final long address, final int index) {
		if (Unsafe.BIG_ENDIAN) {
			return Double.longBitsToDouble(Unsafe.swap(getLong(address, index)));
		}
		return getDouble(address, index);
	}

	/**
	 * 現在のEndianを取得.
	 * 
	 * @return boolean [true]の場合はBigEndianです. [false]の場合はLittalEndianです.
	 */
	public static final boolean getEndian() {
		return Unsafe.BIG_ENDIAN;
	}

	/**
	 * DirectByteBufferのNativeアドレスを取得.
	 * 
	 * @param buf
	 *            対象のByteBufferオブジェクトを設定します.
	 * @return long Nativeアドレスが返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final long getDirectByteBufferAddress(final ByteBuffer buf) throws Exception {
		return Unsafe.getDirectByteBufferAddress(buf);
	}

	/**
	 * UTF8文字列のバイナリ変換長を取得.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @return int バイナリ変換される文字列の長さを設定します.
	 */
	public static final int utf8Length(final String value) {
		return utf8Length(value, 0, value.length());
	}

	/**
	 * UTF8文字列のバイナリ変換長を取得.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return int バイナリ変換される文字列の長さを設定します.
	 */
	public static final int utf8Length(final String value, final int len) {
		return utf8Length(value, 0, len);
	}

	/**
	 * UTF8文字列のバイナリ変換長を取得.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @param off
	 *            文字列のオフセット値を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return int バイナリ変換される文字列の長さを設定します.
	 */
	public static final int utf8Length(final String value, final int off, final int len) {
		int c;
		int ret = 0;
		for (int i = off; i < len; i++) {
			c = (int) value.charAt(i);

			// サロゲートペア処理.
			if (c >= 0xd800 && c <= 0xdbff) {
				c = 0x10000 + (((c - 0xd800) << 10) | ((int) value.charAt(i + 1) - 0xdc00));
				i++;
			}

			if ((c & 0xff80) == 0) {
				ret++;
			} else if (c < 0x800) {
				ret += 2;
			} else if (c < 0x10000) {
				ret += 3;
			} else {
				ret += 4;
			}
		}
		return ret;
	}

	/**
	 * UTF8文字列設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            対象の文字列を設定します.
	 * @return int 変換された文字列の長さを設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final int putUtf8(final long address, final int index, final String value) throws Exception {
		return putUtf8(address, index, value, 0, value.length());
	}

	/**
	 * UTF8文字列設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            対象の文字列を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return int 変換された文字列の長さを設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final int putUtf8(final long address, final int index, final String value, final int len)
			throws Exception {
		return putUtf8(address, index, value, 0, len);
	}

	/**
	 * UTF8文字列設定.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            対象の文字列を設定します.
	 * @param off
	 *            文字列のオフセット値を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return int 変換された文字列の長さを設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final int putUtf8(final long address, final int index, final String value, final int off,
			final int len) throws Exception {
		if (len == 0) {
			return 0;
		}
		int c;
		long p = address + index;
		if (UnsafeMode) {
			for (int i = off; i < len; i++) {
				c = (int) value.charAt(i);

				// サロゲートペア処理.
				if (c >= 0xd800 && c <= 0xdbff) {
					c = 0x10000 + (((c - 0xd800) << 10) | ((int) value.charAt(i + 1) - 0xdc00));
					i++;
				}

				if ((c & 0xff80) == 0) {
					unsafe.putByte(p, (byte) c);
					p++;
				} else if (c < 0x800) {
					unsafe.putByte(p, (byte) ((c >> 6) | 0xc0));
					unsafe.putByte(p + 1, (byte) ((c & 0x3f) | 0x80));
					p += 2;
				} else if (c < 0x10000) {
					unsafe.putByte(p, (byte) ((c >> 12) | 0xe0));
					unsafe.putByte(p + 1, (byte) (((c >> 6) & 0x3f) | 0x80));
					unsafe.putByte(p + 2, (byte) ((c & 0x3f) | 0x80));
					p += 3;
				} else {
					unsafe.putByte(p, (byte) ((c >> 18) | 0xf0));
					unsafe.putByte(p + 1, (byte) (((c >> 12) & 0x3f) | 0x80));
					unsafe.putByte(p + 2, (byte) (((c >> 6) & 0x3f) | 0x80));
					unsafe.putByte(p + 3, (byte) ((c & 0x3f) | 0x80));
					p += 4;
				}

			}
		} else {
			for (int i = off; i < len; i++) {
				c = (int) value.charAt(i);

				// サロゲートペア処理.
				if (c >= 0xd800 && c <= 0xdbff) {
					c = 0x10000 + (((c - 0xd800) << 10) | ((int) value.charAt(i + 1) - 0xdc00));
					i++;
				}

				if ((c & 0xff80) == 0) {
					jni.putByte(p, (byte) c);
					p++;
				} else if (c < 0x800) {
					jni.putByte(p, (byte) ((c >> 6) | 0xc0));
					jni.putByte(p + 1, (byte) ((c & 0x3f) | 0x80));
					p += 2;
				} else if (c < 0x10000) {
					jni.putByte(p, (byte) ((c >> 12) | 0xe0));
					jni.putByte(p + 1, (byte) (((c >> 6) & 0x3f) | 0x80));
					jni.putByte(p + 2, (byte) ((c & 0x3f) | 0x80));
					p += 3;
				} else {
					jni.putByte(p, (byte) ((c >> 18) | 0xf0));
					jni.putByte(p + 1, (byte) (((c >> 12) & 0x3f) | 0x80));
					jni.putByte(p + 2, (byte) (((c >> 6) & 0x3f) | 0x80));
					jni.putByte(p + 3, (byte) ((c & 0x3f) | 0x80));
					p += 4;
				}
			}
		}
		return (int) (p - address);
	}

	/**
	 * UTF8文字列取得.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param length
	 *            バイナリの長さを設定します.
	 * @return String 文字列が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final String getUtf8(final long address, final int index, final int length) throws Exception {
		if (length == 0) {
			return "";
		}
		int c, n;
		int cnt = 0;
		long p = address + index;
		char[] buf = new char[length];
		if (UnsafeMode) {
			for (int i = 0; i < length; i++) {
				if (((c = (int) (unsafe.getByte(p) & 0x000000ff)) & 0x80) == 0) {
					n = (int) (c & 255);
					p++;
				} else if ((c >> 5) == 0x06) {
					n = (int) (((c & 0x1f) << 6) | (unsafe.getByte(p + 1) & 0x3f));
					p += 2;
					i += 1;
				} else if ((c >> 4) == 0x0e) {
					n = (int) (((c & 0x0f) << 12) | (((unsafe.getByte(p + 1)) & 0x3f) << 6)
							| ((unsafe.getByte(p + 2)) & 0x3f));
					p += 3;
					i += 2;
				} else {
					n = (int) (((c & 0x07) << 18) | (((unsafe.getByte(p + 1)) & 0x3f) << 12)
							| (((unsafe.getByte(p + 2)) & 0x3f) << 6) | ((unsafe.getByte(p + 3)) & 0x3f));
					p += 4;
					i += 3;
				}

				// サロゲートペア.
				if ((n & 0xffff0000) != 0) {
					n = n - 0x10000;
					buf[cnt] = (char) (0xd800 | (n >> 10));
					buf[cnt + 1] = (char) (0xdc00 | (n & 0x3ff));
					cnt += 2;
				} else {
					buf[cnt++] = (char) n;
				}
			}
		} else {
			for (int i = 0; i < length; i++) {
				if (((c = (int) (jni.getByte(p) & 0x000000ff)) & 0x80) == 0) {
					n = (int) (c & 255);
					p++;
				} else if ((c >> 5) == 0x06) {
					n = (int) (((c & 0x1f) << 6) | (jni.getByte(p + 1) & 0x3f));
					p += 2;
					i += 1;
				} else if ((c >> 4) == 0x0e) {
					n = (int) (((c & 0x0f) << 12) | (((jni.getByte(p + 1)) & 0x3f) << 6)
							| ((jni.getByte(p + 2)) & 0x3f));
					p += 3;
					i += 2;
				} else {
					n = (int) (((c & 0x07) << 18) | (((jni.getByte(p + 1)) & 0x3f) << 12)
							| (((jni.getByte(p + 2)) & 0x3f) << 6) | ((jni.getByte(p + 3)) & 0x3f));
					p += 4;
					i += 3;
				}

				// サロゲートペア.
				if ((n & 0xffff0000) != 0) {
					n = n - 0x10000;
					buf[cnt] = (char) (0xd800 | (n >> 10));
					buf[cnt + 1] = (char) (0xdc00 | (n & 0x3ff));
					cnt += 2;
				} else {
					buf[cnt++] = (char) n;
				}
			}
		}
		return new String(buf, 0, cnt);
	}

	/**
	 * UTF16文字列のバイナリ変換長を取得.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @return int バイナリ変換される文字列の長さを設定します.
	 */
	public static final int utf16Length(final String value) {
		return value == null || value.length() == 0 ? 0 : (value.length() << 1);
	}

	/**
	 * UTF16文字列のバイナリ変換長を取得.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return int バイナリ変換される文字列の長さを設定します.
	 */
	public static final int utf16Length(final String value, final int len) {
		return len == 0 ? 0 : (len << 1);
	}

	/**
	 * UTF16文字列のバイナリ変換長を取得.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @param off
	 *            文字列のオフセット値を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return int バイナリ変換される文字列の長さを設定します.
	 */
	public static final int utf16Length(final String value, final int off, final int len) {
		return len == 0 ? 0 : (len << 1);
	}

	/**
	 * utf16変換.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            対象の文字列を設定します.
	 * @return int 変換された文字列の長さを設定します. この値は別にこの戻り値を使わなくても、 ( len << 1 ) + 2 or ( len
	 *         * 2 ) + 2 で値は求められます.
	 * @exception Exception
	 *                例外.
	 */
	public static final int putUtf16(final long address, final int index, final String value) throws Exception {
		return putUtf16(address, index, value, 0, value.length());
	}

	/**
	 * utf16変換.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            対象の文字列を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return int 変換された文字列の長さを設定します. この値は別にこの戻り値を使わなくても、 ( len << 1 ) + 2 or ( len
	 *         * 2 ) + 2 で値は求められます.
	 * @exception Exception
	 *                例外.
	 */
	public static final int putUtf16(final long address, final int index, final String value, final int len)
			throws Exception {
		return putUtf16(address, index, value, 0, len);
	}

	/**
	 * utf16変換.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            対象の文字列を設定します.
	 * @param off
	 *            文字列のオフセット値を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return int 変換された文字列の長さを設定します. この値は別にこの戻り値を使わなくても、 ( len << 1 ) + 2 or ( len
	 *         * 2 ) + 2 で値は求められます.
	 * @exception Exception
	 *                例外.
	 */
	public static final int putUtf16(final long address, final int index, final String value, final int off,
			final int len) throws Exception {
		if (len == 0) {
			return 0;
		}
		// BOMなし(BigEndian)で処理.
		long p = address + index;
		char c;
		if (UnsafeMode) {
			for (int i = off; i < len; i++) {
				c = value.charAt(i);
				unsafe.putByte(p, (byte) ((c & 0xff00) >> 8));
				unsafe.putByte(p + 1, (byte) c);
				p += 2;
			}
		} else {
			for (int i = off; i < len; i++) {
				c = value.charAt(i);
				jni.putByte(p, (byte) ((c & 0xff00) >> 8));
				jni.putByte(p + 1, (byte) c);
				p += 2;
			}
		}
		return len << 1;
	}

	/**
	 * utf16変換.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param length
	 *            バイナリの長さを設定します.
	 * @return String 文字列が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final String getUtf16(final long address, final int index, final int length) throws Exception {
		if (length == 0) {
			return "";
		}
		int a, b;
		char[] buf;
		int cnt = 0;
		long p = address + index;
		int len = (length - 2) >> 1;
		if (UnsafeMode) {

			// BOMを取得.
			a = (int) (unsafe.getByte(p) & 255);
			b = (int) (unsafe.getByte(p + 1) & 255);
			p += 2;

			// Little-Endian(Intel系).
			if (a == 0xff && b == 0xfe) {
				buf = new char[len];
				for (int i = 0; i < len; i++) {
					buf[cnt++] = (char) (((unsafe.getByte(p + 1) & 255) << 8) | (unsafe.getByte(p) & 255));
					p += 2;
				}
			}
			// Big-Endian(PowerPC等).
			else if (a == 0xfe && b == 0xff) {
				buf = new char[len];
				for (int i = 0; i < len; i++) {
					buf[cnt++] = (char) (((unsafe.getByte(p) & 255) << 8) | (unsafe.getByte(p + 1) & 255));
					p += 2;
				}
			}
			// BOMなし(BigEndian).
			else {
				len = length >> 1;
				buf = new char[len];
				buf[cnt++] = (char) (((a & 255) << 8) | (b & 255));
				for (int i = 1; i < len; i++) {
					buf[cnt++] = (char) (((unsafe.getByte(p) & 255) << 8) | (unsafe.getByte(p + 1) & 255));
					p += 2;
				}
			}
		} else {

			// BOMを取得.
			a = (int) (jni.getByte(p) & 255);
			b = (int) (jni.getByte(p + 1) & 255);
			p += 2;

			// Little-Endian(Intel系).
			if (a == 0xff && b == 0xfe) {
				buf = new char[len];
				for (int i = 0; i < len; i++) {
					buf[cnt++] = (char) (((jni.getByte(p + 1) & 255) << 8) | (jni.getByte(p) & 255));
					p += 2;
				}
			}
			// Big-Endian(PowerPC等).
			else if (a == 0xfe && b == 0xff) {
				buf = new char[len];
				for (int i = 0; i < len; i++) {
					buf[cnt++] = (char) (((jni.getByte(p) & 255) << 8) | (jni.getByte(p + 1) & 255));
					p += 2;
				}
			}
			// BOMなし(BigEndian).
			else {
				len = length >> 1;
				buf = new char[len];
				buf[cnt++] = (char) (((a & 255) << 8) | (b & 255));
				for (int i = 1; i < len; i++) {
					buf[cnt++] = (char) (((jni.getByte(p) & 255) << 8) | (jni.getByte(p + 1) & 255));
					p += 2;
				}
			}
		}
		return new String(buf, 0, len);
	}
}
