package org.maachang.leveldb;

import static org.maachang.leveldb.JniIO.UnsafeMode;
import static org.maachang.leveldb.JniIO.unsafe;

/**
 * 文字列のNative変換.
 */
public class NativeString {
	private NativeString() {}
	
	/**
	 * 文字列変換後の長さを取得.
	 * @param value
	 * @return
	 */
	public static final int nativeLength(String value) {
		return utf8Length(value);
		//return utf16Length(value);
	}
	
	/**
	 * 文字列のNative変換.
	 * @param addr
	 * @param idx
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public static final int toNative(long addr, int idx, String value)
		throws Exception {
		return toNative(addr, idx, value, 0, value == null ? 0 : value.length());
	}
	
	/**
	 * 文字列のNative変換.
	 * @param addr
	 * @param idx
	 * @param value
	 * @param off
	 * @param len
	 * @return
	 * @throws Exception
	 */
	public static final int toNative(long addr, int idx, String value, int off, int len)
		throws Exception {
		return putUtf8(addr, idx, value, off, len);
		//return putUtf16(addr, idx, value, off, len);
	}
	
	/**
	 * 文字列のNative変換.
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public static final byte[] toNative(String value)
		throws Exception {
		return toNative(value, 0, value == null ? 0 : value.length());
	}

	
	/**
	 * 文字列のNative変換.
	 * @param value
	 * @param off
	 * @param len
	 * @return
	 * @throws Exception
	 */
	public static final byte[] toNative(String value, int off, int len)
		throws Exception {
		return putUtf8(value, off, len);
		//return putUtf16(value, off, len);
	}

	
	/**
	 * Native文字列をJavaの文字列変換.
	 * @param addr
	 * @param idx
	 * @param len
	 * @return
	 * @throws Exception
	 */
	public static final String toJava(long addr, int idx, int len)
		throws Exception {
		return getUtf8(addr, idx, len);
		//return getUtf16(addr, idx, len);
	}
	
	/**
	 * Native文字列をJavaの文字列変換.
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 * @throws Exception
	 */
	public static final String toJava(byte[] b, int off, int len)
		throws Exception {
		return getUtf8(b, off, len);
		//return getUtf16(b, off, len);
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
	 * utf16変換して、Nativeメモリに書き込み.
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
	public static final int putUtf16(final long address, final int index, final String value)
		throws Exception {
		return putUtf16(address, index, value, 0, value == null ? 0 : value.length());
	}

	/**
	 * utf16変換して、Nativeメモリに書き込み.
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
	 * utf16変換して、Nativeメモリに書き込み.
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
	 * @return int 変換された文字列の長さを設定します. この値は別にこの戻り値を使わなくても、
	 *             ( len << 1 ) + 2 or ( len * 2 ) + 2 で値は求められます.
	 * @exception Exception
	 *                例外.
	 */
	public static final int putUtf16(final long address, final int index, final String value, final int off, final int len)
		throws Exception {
		if (len == 0) {
			return 0;
		}
		// BOMなし(BigEndian)で処理.
		long p = address + index;
		char c;
		if (UnsafeMode) {
			for (int i = 0; i < len; i++) {
				c = value.charAt(off + i);
				unsafe.putByte(p, (byte) ((c & 0xff00) >> 8));
				unsafe.putByte(p + 1, (byte)(c & 0x00ff));
				p += 2;
			}
		} else {
			for (int i = 0; i < len; i++) {
				c = value.charAt(off + i);
				jni.putByte(p, (byte) ((c & 0xff00) >> 8));
				jni.putByte(p + 1, (byte)(c & 0x00ff));
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
	public static final int utf8Length(final String value, final int off,
		final int len) {
		if(value == null || value.length() == 0) {
			return 0;
		}
		int c;
		int ret = 0;
		for (int i = 0; i < len; i++) {
			c = (int) value.charAt(off + i);
			// サロゲートペア処理.
			if (c >= 0xd800 && c <= 0xdbff) {
				c = 0x10000 + (((c - 0xd800) << 10) | ((int) value.charAt(off + i + 1) - 0xdc00));
				i ++;
			}
			if ((c & 0xffffff80) == 0) {
				ret += 1;
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
	 * UTF8文字列変換.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            対象の文字列を設定します.
	 * @return int 変換された文字列のバイナリ長が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final int putUtf8(final long address, final int index, final String value)
		throws Exception {
		return putUtf8(address, index, value, 0, value == null ? 0 : value.length());
	}

	/**
	 * UTF8文字列変換.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @param index
	 *            対象のインデックス位置を設定します.
	 * @param value
	 *            対象の文字列を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return int 変換された文字列のバイナリ長が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final int putUtf8(final long address, final int index, final String value, final int len)
		throws Exception {
		return putUtf8(address, index, value, 0, len);
	}

	/**
	 * UTF8文字列変換.
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
	 * @return int 変換された文字列のバイナリ長が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final int putUtf8(final long address, final int index, final String value, final int off, final int len)
		throws Exception {
		if (value == null || len == 0) {
			return 0;
		}
		int c;
		long p = address + index;
		if (UnsafeMode) {
			for (int i = 0; i < len; i++) {
				c = (int) value.charAt(off + i) & 0x0000ffff;

				// サロゲートペア処理.
				if (c >= 0xd800 && c <= 0xdbff) {
					c = 0x10000 + (((c - 0xd800) << 10) | ((int) value.charAt(off + i + 1) - 0xdc00));
					i ++;
				}

				if ((c & 0xffffff80) == 0) {
					unsafe.putByte(p ++, (byte) c);
				} else if (c < 0x800) {
					unsafe.putByte(p ++, (byte) ((c >> 6) | 0xc0));
					unsafe.putByte(p ++, (byte) ((c & 0x3f) | 0x80));
				} else if (c < 0x10000) {
					unsafe.putByte(p ++, (byte) ((c >> 12) | 0xe0));
					unsafe.putByte(p ++, (byte) (((c >> 6) & 0x3f) | 0x80));
					unsafe.putByte(p ++, (byte) ((c & 0x3f) | 0x80));
				} else {
					unsafe.putByte(p ++, (byte) ((c >> 18) | 0xf0));
					unsafe.putByte(p ++, (byte) (((c >> 12) & 0x3f) | 0x80));
					unsafe.putByte(p ++, (byte) (((c >> 6) & 0x3f) | 0x80));
					unsafe.putByte(p ++, (byte) ((c & 0x3f) | 0x80));
				}

			}
		} else {
			for (int i = 0; i < len; i++) {
				c = (int) value.charAt(off + i) & 0x0000ffff;

				// サロゲートペア処理.
				if (c >= 0xd800 && c <= 0xdbff) {
					c = 0x10000 + (((c - 0xd800) << 10) | ((int) value.charAt(off + i + 1) - 0xdc00));
					i++;
				}

				if ((c & 0xffffff80) == 0) {
					jni.putByte(p ++, (byte) c);
				} else if (c < 0x800) {
					jni.putByte(p ++, (byte) ((c >> 6) | 0xc0));
					jni.putByte(p ++, (byte) ((c & 0x3f) | 0x80));
				} else if (c < 0x10000) {
					jni.putByte(p ++, (byte) ((c >> 12) | 0xe0));
					jni.putByte(p ++, (byte) (((c >> 6) & 0x3f) | 0x80));
					jni.putByte(p ++, (byte) ((c & 0x3f) | 0x80));
				} else {
					jni.putByte(p ++, (byte) ((c >> 18) | 0xf0));
					jni.putByte(p ++, (byte) (((c >> 12) & 0x3f) | 0x80));
					jni.putByte(p ++, (byte) (((c >> 6) & 0x3f) | 0x80));
					jni.putByte(p ++, (byte) ((c & 0x3f) | 0x80));
				}
			}
		}
		return (int) (p - (address + index));
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
	public static final String getUtf8(final long address, final int index, final int length)
		throws Exception {
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
					p += 1;
				} else if ((c >> 5) == 0x06) {
					n = (int) (((c & 0x1f) << 6) | (unsafe.getByte(p + 1) & 0x3f));
					p += 2;
					i += 1;
				} else if ((c >> 4) == 0x0e) {
					n = (int) (((c & 0x0f) << 12)
						| (((unsafe.getByte(p + 1)) & 0x3f) << 6) | ((unsafe.getByte(p + 2)) & 0x3f));
					p += 3;
					i += 2;
				} else {
					n = (int) (((c & 0x07) << 18)
						| (((unsafe.getByte(p + 1)) & 0x3f) << 12)
						| (((unsafe.getByte(p + 2)) & 0x3f) << 6) | ((unsafe.getByte(p + 3)) & 0x3f));
					p += 4;
					i += 3;
				}

				// サロゲートペア.
				if ((n & 0xffff0000) != 0) {
					n -= 0x10000;
					buf[cnt ++] = (char) (0xd800 | (n >> 10));
					buf[cnt ++] = (char) (0xdc00 | (n & 0x3ff));
				} else {
					buf[cnt++] = (char) n;
				}
			}
		} else {
			for (int i = 0; i < length; i++) {
				if (((c = (int) (jni.getByte(p) & 0x000000ff)) & 0x80) == 0) {
					n = (int) (c & 255);
					p += 1;
				} else if ((c >> 5) == 0x06) {
					n = (int) (((c & 0x1f) << 6) | (jni.getByte(p + 1) & 0x3f));
					p += 2;
					i += 1;
				} else if ((c >> 4) == 0x0e) {
					n = (int) (((c & 0x0f) << 12)
						| (((jni.getByte(p + 1)) & 0x3f) << 6) | ((jni.getByte(p + 2)) & 0x3f));
					p += 3;
					i += 2;
				} else {
					n = (int) (((c & 0x07) << 18)
						| (((jni.getByte(p + 1)) & 0x3f) << 12)
						| (((jni.getByte(p + 2)) & 0x3f) << 6) | ((jni.getByte(p + 3)) & 0x3f));
					p += 4;
					i += 3;
				}

				// サロゲートペア.
				if ((n & 0xffff0000) != 0) {
					n -= 0x10000;
					buf[cnt ++] = (char) (0xd800 | (n >> 10));
					buf[cnt ++] = (char) (0xdc00 | (n & 0x3ff));
				} else {
					buf[cnt ++] = (char) n;
				}
			}
		}
		return new String(buf, 0, cnt);
	}
	
	/**
	 * UTF16文字列変換.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @return byte[] 変換結果が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final byte[] putUtf16(final String value)
		throws Exception {
		return putUtf16(value, 0, value == null ? 0 : value.length());
	}
	
	/**
	 * UTF16文字列変換.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return byte[] 変換結果が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final byte[] putUtf16(final String value, final int len)
		throws Exception {
		return putUtf16(value, 0, len);
	}
	
	/**
	 * UTF16文字列変換.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @param off
	 *            文字列のオフセット値を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return byte[] 変換されたバイナリが返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final byte[] putUtf16(final String value, final int off, final int len)
		throws Exception {
		if (value == null || len == 0) {
			return new byte[0];
		}
		// BOMなし(BigEndian)で処理.
		int p = 0;
		char c;
		byte[] ret = new byte[len << 1];
		for (int i = 0; i < len; i++) {
			c = value.charAt(off + i);
			ret[p] = (byte) ((c & 0xff00) >> 8);
			ret[p + 1] = (byte)(c & 0x00ff);
			p += 2;
		}
		return ret;
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
	public static final String getUtf16(final byte[] bin, final int off, final int length)
		throws Exception {
		if (bin == null || length == 0) {
			return "";
		}
		int a, b;
		char[] buf;
		int cnt = 0;
		int p = off;
		int len = (length - 2) >> 1;
		
		// BOMを取得.
		a = (int) (bin[p] & 255);
		b = (int) (bin[p + 1] & 255);
		p += 2;

		// Little-Endian(Intel系).
		if (a == 0xff && b == 0xfe) {
			buf = new char[len];
			for (int i = 0; i < len; i++) {
				buf[cnt++] = (char) (((bin[p + 1] & 255) << 8) | (bin[p] & 255));
				p += 2;
			}
		}
		// Big-Endian(PowerPC等).
		else if (a == 0xfe && b == 0xff) {
			buf = new char[len];
			for (int i = 0; i < len; i++) {
				buf[cnt++] = (char) (((bin[p] & 255) << 8) | (bin[p + 1] & 255));
				p += 2;
			}
		}
		// BOMなし(BigEndian).
		else {
			len = length >> 1;
			buf = new char[len];
			buf[cnt++] = (char) (((a & 255) << 8) | (b & 255));
			for (int i = 1; i < len; i++) {
				buf[cnt++] = (char) (((bin[p] & 255) << 8) | (bin[p + 1] & 255));
				p += 2;
			}
		}
		return new String(buf, 0, len);
	}
	
	/**
	 * UTF8文字列変換.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @return byte[] 変換結果が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final byte[] putUtf8(final String value)
		throws Exception {
		return putUtf8(value, 0, value == null ? 0 : value.length());
	}
	
	/**
	 * UTF8文字列変換.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return byte[] 変換結果が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final byte[] putUtf8(final String value, final int len)
		throws Exception {
		return putUtf8(value, 0, len);
	}
	
	/**
	 * UTF8文字列変換.
	 * 
	 * @param value
	 *            対象の文字列を設定します.
	 * @param off
	 *            文字列のオフセット値を設定します.
	 * @param len
	 *            文字列の長さを設定します.
	 * @return byte[] 変換結果が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final byte[] putUtf8(final String value, final int off, final int len)
		throws Exception {
		if (value == null || len == 0) {
			return new byte[0];
		}
		int c;
		int p = 0;
		byte[] ret = new byte[utf8Length(value, off, len)];
		for (int i = 0; i < len; i++) {
			c = (int) value.charAt(off + i) & 0x0000ffff;
			// サロゲートペア処理.
			if (c >= 0xd800 && c <= 0xdbff) {
				c = 0x10000 + (((c - 0xd800) << 10) | ((int) value.charAt(off + i + 1) - 0xdc00));
				i ++;
			}
			if ((c & 0xffffff80) == 0) {
				ret[p ++] = (byte) c;
			} else if (c < 0x800) {
				ret[p ++] = (byte) ((c >> 6) | 0xc0);
				ret[p ++] = (byte) ((c & 0x3f) | 0x80);
			} else if (c < 0x10000) {
				ret[p ++] = (byte) ((c >> 12) | 0xe0);
				ret[p ++] = (byte) (((c >> 6) & 0x3f) | 0x80);
				ret[p ++] = (byte) ((c & 0x3f) | 0x80);
			} else {
				ret[p ++] = (byte) ((c >> 18) | 0xf0);
				ret[p ++] = (byte) (((c >> 12) & 0x3f) | 0x80);
				ret[p ++] = (byte) (((c >> 6) & 0x3f) | 0x80);
				ret[p ++] = (byte) ((c & 0x3f) | 0x80);
			}
		}
		return ret;
	}
	
	/**
	 * UTF8文字列取得.
	 * 
	 * @param b
	 *            対象のバイナリを設定します.
	 * @param off
	 *            対象のインデックス位置を設定します.
	 * @param len
	 *            バイナリの長さを設定します.
	 * @return String 文字列が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final String getUtf8(final byte[] b, final int off, final int len)
		throws Exception {
		if (b == null || len == 0) {
			return "";
		}
		int c, n;
		int cnt = 0;
		int p = off;
		char[] buf = new char[len];
		for (int i = 0; i < len; i++) {
			if (((c = (int) (b[p] & 0x000000ff)) & 0x80) == 0) {
				n = (int) (c & 255);
				p += 1;
			} else if ((c >> 5) == 0x06) {
				n = (int) (((c & 0x1f) << 6) | (b[p + 1] & 0x3f));
				p += 2;
				i += 1;
			} else if ((c >> 4) == 0x0e) {
				n = (int) (((c & 0x0f) << 12)
					| ((b[p + 1] & 0x3f) << 6) | (b[p + 2] & 0x3f));
				p += 3;
				i += 2;
			} else {
				n = (int) (((c & 0x07) << 18)
					| ((b[p + 1] & 0x3f) << 12)
					| ((b[p + 2] & 0x3f) << 6) | (b[p + 3] & 0x3f));
				p += 4;
				i += 3;
			}
			// サロゲートペア.
			if ((n & 0xffff0000) != 0) {
				n -= 0x10000;
				buf[cnt ++] = (char) (0xd800 | (n >> 10));
				buf[cnt ++] = (char) (0xdc00 | (n & 0x3ff));
			} else {
				buf[cnt++] = (char) n;
			}
		}
		return new String(buf, 0, cnt);
	}
}
