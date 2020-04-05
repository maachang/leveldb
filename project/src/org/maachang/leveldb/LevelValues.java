package org.maachang.leveldb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.maachang.leveldb.types.IntBin;
import org.maachang.leveldb.types.IntInt;
import org.maachang.leveldb.types.IntLong;
import org.maachang.leveldb.types.IntStr;
import org.maachang.leveldb.types.LongBin;
import org.maachang.leveldb.types.LongInt;
import org.maachang.leveldb.types.LongLong;
import org.maachang.leveldb.types.LongStr;
import org.maachang.leveldb.types.StrBin;
import org.maachang.leveldb.types.StrInt;
import org.maachang.leveldb.types.StrLong;
import org.maachang.leveldb.types.StrStr;
import org.maachang.leveldb.types.TwoKey;
import org.maachang.leveldb.util.ArrayMap;
import org.maachang.leveldb.util.ObjectList;

/**
 * Leveldb Valueデータ変換処理.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public final class LevelValues {
	protected LevelValues() {
	}
	
	/** 拡張変換処理. **/
	private static OriginCode ORIGIN_CODE = null;
	
	/**
	 * 拡張変換処理を追加.
	 * @param code
	 */
	public static final void setOriginCode(OriginCode code) {
		ORIGIN_CODE = code;
	}

	/**
	 * オブジェクトをバイナリに変換.
	 * 
	 * @parma buf 対象のバッファオブジェクトを設定します.
	 * @param o
	 *            対象のオブジェクトを設定します.
	 * @return byte[] 変換されたバイナリ情報が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final void encode(JniBuffer buf, Object o) throws Exception {

		// オブジェクト変換.
		encodeObject(buf, o);
	}

	/**
	 * バイナリをオブジェクトに変換.
	 * 
	 * @param b
	 *            対象のJNIバッファを設定します.
	 * @return Object 変換されたオブジェクトが返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final Object decode(JniBuffer b) throws Exception {
		return decode(b, 0, b.position());
	}

	/**
	 * バイナリをオブジェクトに変換.
	 * 
	 * @param b
	 *            対象のJNIバッファを設定します.
	 * @param off
	 *            対象のオフセット値を設定します.
	 * @param len
	 *            対象の長さを設定します.
	 * @return Object 変換されたオブジェクトが返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final Object decode(JniBuffer b, int off, int len) throws Exception {
		if (len > b.position()) {
			throw new IllegalArgumentException("The specified length is out of range:"
				+ len + "," + b.position());
		}
		int[] p = new int[] { off };
		return decodeObject(p, b, len);
	}

	/**
	 * バイナリをオブジェクトに変換.
	 * 
	 * @param outOff
	 *            対象のオフセット値を設定します.
	 * @param b
	 *            対象のJNIバッファを設定します.
	 * @param len
	 *            対象の長さを設定します.
	 * @return Object 変換されたオブジェクトが返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final Object decodeBinary(int[] outOff, JniBuffer b, int len) throws Exception {
		if (len > b.position()) {
			throw new IllegalArgumentException("The specified length is out of range:"
				+ len + "," + b.position());
		}
		return decodeObject(outOff, b, len);
	}

	/** 1バイトバイナリ変換. **/
	public static final void byte1(JniBuffer buf, int b) throws Exception {
		buf.write((b & 0xff));
	}

	/** 2バイトバイナリ変換. **/
	public static final void byte2(JniBuffer buf, int b) throws Exception {
		buf.write(new byte[] { (byte) ((b & 0xff00) >> 8), (byte) (b & 0xff) });
	}

	/** 4バイトバイナリ変換. **/
	public static final void byte4(JniBuffer buf, int b) throws Exception {
		// 4バイトの場合は、先頭2ビットをビット長とする.
		int bit = nlzs(b);
		int src = (bit >> 3) + ((bit & 1) | ((bit >> 1) & 1) | ((bit >> 2) & 1));
		bit = ((bit += 2) >> 3) + ((bit & 1) | ((bit >> 1) & 1) | ((bit >> 2) & 1));

		// 先頭2ビット条件が混同できる場合.
		if (bit == src) {
			switch (bit) {
			case 1:
				buf.write(new byte[] { (byte) (b & 0xff) });
				return;
			case 2:
				buf.write(new byte[] { (byte) (0x40 | ((b & 0xff00) >> 8)), (byte) (b & 0xff) });
				return;
			case 3:
				buf.write(new byte[] { (byte) (0x80 | ((b & 0xff0000) >> 16)), (byte) ((b & 0xff00) >> 8),
						(byte) (b & 0xff) });
				return;
			case 4:
				buf.write(new byte[] { (byte) (0xc0 | ((b & 0xff000000) >> 24)), (byte) ((b & 0xff0000) >> 16),
						(byte) ((b & 0xff00) >> 8), (byte) (b & 0xff) });
				return;
			}
		}
		// 先頭2ビット条件が混同できない場合.
		switch (src) {
		case 0:
		case 1:
			buf.write(new byte[] { (byte) 0, (byte) (b & 0xff) });
			return;
		case 2:
			buf.write(new byte[] { (byte) 0x40, (byte) ((b & 0xff00) >> 8), (byte) (b & 0xff) });
			return;
		case 3:
			buf.write(new byte[] { (byte) 0x80, (byte) ((b & 0xff0000) >> 16), (byte) ((b & 0xff00) >> 8),
					(byte) (b & 0xff) });
			return;
		case 4:
			buf.write(new byte[] { (byte) 0xc0, (byte) ((b & 0xff000000) >> 24), (byte) ((b & 0xff0000) >> 16),
					(byte) ((b & 0xff00) >> 8), (byte) (b & 0xff) });
			return;
		}
	}

	/** 8バイトバイナリ変換. **/
	public static final void byte8(JniBuffer buf, long b) throws Exception {
		// 8バイトの場合は、先頭3ビットをビット長とする.
		int bit = nlzs(b);
		int src = (bit >> 3) + ((bit & 1) | ((bit >> 1) & 1) | ((bit >> 2) & 1));
		bit = ((bit += 3) >> 3) + ((bit & 1) | ((bit >> 1) & 1) | ((bit >> 2) & 1));

		// 先頭3ビット条件が混同できる場合.
		if (bit == src) {
			switch (bit) {
			case 1:
				buf.write(new byte[] { (byte) (b & 0xffL) });
				return;
			case 2:
				buf.write(new byte[] { (byte) (0x20 | ((b & 0xff00L) >> 8L)), (byte) (b & 0xffL) });
				return;
			case 3:
				buf.write(new byte[] { (byte) (0x40 | ((b & 0xff0000L) >> 16L)), (byte) ((b & 0xff00L) >> 8L),
						(byte) (b & 0xffL) });
				return;
			case 4:
				buf.write(new byte[] { (byte) (0x60 | ((b & 0xff000000L) >> 24L)), (byte) ((b & 0xff0000L) >> 16L),
						(byte) ((b & 0xff00L) >> 8L), (byte) (b & 0xffL) });
				return;
			case 5:
				buf.write(new byte[] { (byte) (0x80 | ((b & 0xff00000000L) >> 32L)), (byte) ((b & 0xff000000L) >> 24L),
						(byte) ((b & 0xff0000L) >> 16L), (byte) ((b & 0xff00L) >> 8L), (byte) (b & 0xffL) });
				return;
			case 6:
				buf.write(new byte[] { (byte) (0xA0 | ((b & 0xff0000000000L) >> 40L)),
						(byte) ((b & 0xff00000000L) >> 32L), (byte) ((b & 0xff000000L) >> 24L),
						(byte) ((b & 0xff0000L) >> 16L), (byte) ((b & 0xff00L) >> 8L), (byte) (b & 0xffL) });
				return;
			case 7:
				buf.write(new byte[] { (byte) (0xC0 | ((b & 0xff000000000000L) >> 48L)),
						(byte) ((b & 0xff0000000000L) >> 40L), (byte) ((b & 0xff00000000L) >> 32L),
						(byte) ((b & 0xff000000L) >> 24L), (byte) ((b & 0xff0000L) >> 16L),
						(byte) ((b & 0xff00L) >> 8L), (byte) (b & 0xffL) });
				return;
			case 8:
				buf.write(new byte[] { (byte) (0xE0 | ((b & 0xff00000000000000L) >> 56L)),
						(byte) ((b & 0xff000000000000L) >> 48L), (byte) ((b & 0xff0000000000L) >> 40L),
						(byte) ((b & 0xff00000000L) >> 32L), (byte) ((b & 0xff000000L) >> 24L),
						(byte) ((b & 0xff0000L) >> 16L), (byte) ((b & 0xff00L) >> 8L), (byte) (b & 0xffL) });
				return;
			}
		}
		// 先頭3ビット条件が混同できない場合.
		switch (src) {
		case 0:
		case 1:
			buf.write(new byte[] { (byte) 0, (byte) (b & 0xffL) });
			return;
		case 2:
			buf.write(new byte[] { (byte) 0x20, (byte) ((b & 0xff00L) >> 8L), (byte) (b & 0xffL) });
			return;
		case 3:
			buf.write(new byte[] { (byte) 0x40, (byte) ((b & 0xff0000L) >> 16L), (byte) ((b & 0xff00L) >> 8L),
					(byte) (b & 0xffL) });
			return;
		case 4:
			buf.write(new byte[] { (byte) 0x60, (byte) ((b & 0xff000000L) >> 24L), (byte) ((b & 0xff0000L) >> 16L),
					(byte) ((b & 0xff00L) >> 8L), (byte) (b & 0xffL) });
			return;
		case 5:
			buf.write(new byte[] { (byte) 0x80, (byte) ((b & 0xff00000000L) >> 32L), (byte) ((b & 0xff000000L) >> 24L),
					(byte) ((b & 0xff0000L) >> 16L), (byte) ((b & 0xff00L) >> 8L), (byte) (b & 0xffL) });
			return;
		case 6:
			buf.write(new byte[] { (byte) 0xA0, (byte) ((b & 0xff0000000000L) >> 40L),
					(byte) ((b & 0xff00000000L) >> 32L), (byte) ((b & 0xff000000L) >> 24L),
					(byte) ((b & 0xff0000L) >> 16L), (byte) ((b & 0xff00L) >> 8L), (byte) (b & 0xffL) });
			return;
		case 7:
			buf.write(new byte[] { (byte) 0xC0, (byte) ((b & 0xff000000000000L) >> 48L),
					(byte) ((b & 0xff0000000000L) >> 40L), (byte) ((b & 0xff00000000L) >> 32L),
					(byte) ((b & 0xff000000L) >> 24L), (byte) ((b & 0xff0000L) >> 16L), (byte) ((b & 0xff00L) >> 8L),
					(byte) (b & 0xffL) });
			return;
		case 8:
			buf.write(new byte[] { (byte) 0xE0, (byte) ((b & 0xff00000000000000L) >> 56L),
					(byte) ((b & 0xff000000000000L) >> 48L), (byte) ((b & 0xff0000000000L) >> 40L),
					(byte) ((b & 0xff00000000L) >> 32L), (byte) ((b & 0xff000000L) >> 24L),
					(byte) ((b & 0xff0000L) >> 16L), (byte) ((b & 0xff00L) >> 8L), (byte) (b & 0xffL) });
			return;
		}
	}

	/**
	 * 文字バイナリ変換.
	 * 
	 * @param buf
	 *            対象のバッファを設定します.
	 * @param s
	 *            対象の情報を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final void stringBinary(JniBuffer buf, String s) throws Exception {
		int len = JniIO.utf16Length(s);
		buf.recreate(true, buf.position() + len + 7);
		byte4(buf, len);
		if (len != 0) {
			buf.addPosition(JniIO.putUtf16(buf.address(), buf.position(), s));
		}
	}

	/**
	 * LevelArrayバイナリ変換.
	 * 
	 * @param buf
	 *            対象のバッファを設定します.
	 * @param ary
	 *            対象の情報を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final void levelArrayBinary(JniBuffer buf, LevelArray ary) throws Exception {
		int i;
		Object[] a, b;
		int len = ary.length;

		// データ長.
		byte4(buf, len);

		// データ変換.
		a = ary.keyList;
		b = ary.dataList;
		for (i = 0; i < len; i++) {
			encodeObject(buf, a[i]);
			encodeObject(buf, b[i]);
		}
	}

	/**
	 * 2キー情報バイナリ変換.
	 * 
	 * @param buf
	 *            対象のバッファを設定します.
	 * @param twoKey
	 *            対象の2キー情報を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final void twoKeyBinary(JniBuffer buf, TwoKey twoKey) throws Exception {

		byte4(buf, twoKey.toBufferLength());
		twoKey.toBuffer(buf);
	}

	/**
	 * シリアライズ変換.
	 * 
	 * @param buf
	 *            対象のバッファを設定します.
	 * @param s
	 *            対象の情報を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final void serialBinary(JniBuffer buf, Serializable s) throws Exception {
		byte[] b = toBinary(s);
		byte4(buf, b.length); // 長さ.
		buf.write(b, 0, b.length); // body.
	}

	/**
	 * シリアライズオブジェクトをバイナリ変換.
	 * 
	 * @param value
	 *            対象のシリアライズオブジェクトを設定します.
	 * @return byte[] バイナリ変換された内容が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final byte[] toBinary(Serializable value) throws Exception {
		if (value == null) {
			throw new IllegalArgumentException("Argument is invalid");
		}
		byte[] ret = null;
		ObjectOutputStream o = null;
		try {
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			o = new ObjectOutputStream(b);
			o.writeObject(value);
			o.flush();
			ret = b.toByteArray();
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				o.close();
			} catch (Exception e) {
			}
		}
		return ret;
	}

	/** ヘッダ文字セット. **/
	public static final void head(JniBuffer buf, int n) throws Exception {
		byte1(buf, (n & 0x000000ff));
	}
	
	/** ヘッダ文字取得. **/
	public static final int head(long b, int[] off) {
		return byte1Int(b, off) & 0x000000ff;
	}


	/** 1バイト数値変換. **/
	public static final int byte1Int(long b, int[] off) {
		return JniIO.get(b, off[0]++) & 0xff;
	}

	/** 2バイト数値変換. **/
	public static final int byte2Int(long b, int[] off) {
		int ret = ((JniIO.get(b, off[0]) & 0xff) << 8) | (JniIO.get(b, off[0]+1) & 0xff);
		off[0] += 2;
		return ret;
	}

	/** 4バイト数値変換. **/
	public static final int byte4Int(long b, int[] off) {
		int o = off[0];
		int h = JniIO.get(b, o);
		if ((h & 0x3f) == 0) {
			// ヘッダ2ビットが単体１バイト定義の場合.
			switch ((h & 0xc0) >> 6) {
			case 0:
				off[0] += 2;
				return (JniIO.get(b, o + 1) & 0xff);
			case 1:
				off[0] += 3;
				return ((JniIO.get(b, o + 1) & 0xff) << 8) | (JniIO.get(b, o + 2) & 0xff);
			case 2:
				off[0] += 4;
				return ((JniIO.get(b, o + 1) & 0xff) << 16) | ((JniIO.get(b, o + 2) & 0xff) << 8)
						| (JniIO.get(b, o + 3) & 0xff);
			case 3:
				off[0] += 5;
				return ((JniIO.get(b, o + 1) & 0xff) << 24) | ((JniIO.get(b, o + 2) & 0xff) << 16)
						| ((JniIO.get(b, o + 3) & 0xff) << 8) | (JniIO.get(b, o + 4) & 0xff);
			}
			throw new IllegalArgumentException("Invalid byte4Int condition:" + off[0]);
		}
		// ヘッダ2ビットが混在定義の場合.
		switch ((h & 0xc0) >> 6) {
		case 0:
			off[0] += 1;
			return (h & 0x3f);
		case 1:
			off[0] += 2;
			return ((h & 0x3f) << 8) | (JniIO.get(b, o + 1) & 0xff);
		case 2:
			off[0] += 3;
			return ((h & 0x3f) << 16) | ((JniIO.get(b, o + 1) & 0xff) << 8) | (JniIO.get(b, o + 2) & 0xff);
		case 3:
			off[0] += 4;
			return ((h & 0x3f) << 24) | ((JniIO.get(b, o + 1) & 0xff) << 16) | ((JniIO.get(b, o + 2) & 0xff) << 8)
					| (JniIO.get(b, o + 3) & 0xff);
		}
		throw new IllegalArgumentException("Invalid byte4Int condition:" + off[0]);
	}

	/** 8バイト数値変換. **/
	public static final long byte8Long(long b, int[] off) {
		int o = off[0];
		int h = JniIO.get(b, o);
		if ((h & 0x1f) == 0) {
			// ヘッダ3ビットが単体１バイト定義の場合.
			switch ((h & 0xe0) >> 5) {
			case 0:
				off[0] += 2;
				return (long) (JniIO.get(b, o + 1) & 0xff);
			case 1:
				off[0] += 3;
				return (long) (((JniIO.get(b, o + 1) & 0xff) << 8) | (JniIO.get(b, o + 2) & 0xff));
			case 2:
				off[0] += 4;
				return (long) (((JniIO.get(b, o + 1) & 0xff) << 16) | ((JniIO.get(b, o + 2) & 0xff) << 8)
						| (JniIO.get(b, o + 3) & 0xff));
			case 3:
				off[0] += 5;
				return (long) (((JniIO.get(b, o + 1) & 0xff) << 24) | ((JniIO.get(b, o + 2) & 0xff) << 16)
						| ((JniIO.get(b, o + 3) & 0xff) << 8) | (JniIO.get(b, o + 4) & 0xff));
			case 4:
				off[0] += 6;
				return (long) (((JniIO.get(b, o + 1) & 0xffL) << 32L) | ((JniIO.get(b, o + 2) & 0xffL) << 24L)
						| ((JniIO.get(b, o + 3) & 0xffL) << 16L) | ((JniIO.get(b, o + 4) & 0xffL) << 8L)
						| (JniIO.get(b, o + 5) & 0xffL));
			case 5:
				off[0] += 7;
				return (long) (((JniIO.get(b, o + 1) & 0xffL) << 40L) | ((JniIO.get(b, o + 2) & 0xffL) << 32L)
						| ((JniIO.get(b, o + 3) & 0xffL) << 24L) | ((JniIO.get(b, o + 4) & 0xffL) << 16L)
						| ((JniIO.get(b, o + 5) & 0xffL) << 8L) | (JniIO.get(b, o + 6) & 0xffL));
			case 6:
				off[0] += 8;
				return (long) (((JniIO.get(b, o + 1) & 0xffL) << 48L) | ((JniIO.get(b, o + 2) & 0xffL) << 40L)
						| ((JniIO.get(b, o + 3) & 0xffL) << 32L) | ((JniIO.get(b, o + 4) & 0xffL) << 24L)
						| ((JniIO.get(b, o + 5) & 0xffL) << 16L) | ((JniIO.get(b, o + 6) & 0xffL) << 8L)
						| (JniIO.get(b, o + 7) & 0xffL));
			case 7:
				off[0] += 9;
				return (long) (((JniIO.get(b, o + 1) & 0xffL) << 56L) | ((JniIO.get(b, o + 2) & 0xffL) << 48L)
						| ((JniIO.get(b, o + 3) & 0xffL) << 40L) | ((JniIO.get(b, o + 4) & 0xffL) << 32L)
						| ((JniIO.get(b, o + 5) & 0xffL) << 24L) | ((JniIO.get(b, o + 6) & 0xffL) << 16L)
						| ((JniIO.get(b, o + 7) & 0xffL) << 8L) | (JniIO.get(b, o + 8) & 0xffL));
			}
			throw new IllegalArgumentException("Invalid byte8Long condition:" + off[0]);
		}
		// ヘッダ3ビットが混在定義の場合.
		switch ((h & 0xe0) >> 5) {
		case 0:
			off[0] += 1;
			return (long) (h & 0x1f);
		case 1:
			off[0] += 2;
			return (long) (((h & 0x1f) << 8) | (JniIO.get(b, o + 1) & 0xff));
		case 2:
			off[0] += 3;
			return (long) (((h & 0x1f) << 16) | ((JniIO.get(b, o + 1) & 0xff) << 8) | (JniIO.get(b, o + 2) & 0xff));
		case 3:
			off[0] += 4;
			return (long) (((h & 0x1f) << 24) | ((JniIO.get(b, o + 1) & 0xff) << 16)
					| ((JniIO.get(b, o + 2) & 0xff) << 8) | (JniIO.get(b, o + 3) & 0xff));
		case 4:
			off[0] += 5;
			return (long) (((h & 0x1fL) << 32L) | ((JniIO.get(b, o + 1) & 0xffL) << 24L)
					| ((JniIO.get(b, o + 2) & 0xffL) << 16L) | ((JniIO.get(b, o + 3) & 0xffL) << 8L)
					| (JniIO.get(b, o + 4) & 0xffL));
		case 5:
			off[0] += 6;
			return (long) (((h & 0x1fL) << 40L) | ((JniIO.get(b, o + 1) & 0xffL) << 32L)
					| ((JniIO.get(b, o + 2) & 0xffL) << 24L) | ((JniIO.get(b, o + 3) & 0xffL) << 16L)
					| ((JniIO.get(b, o + 4) & 0xffL) << 8L) | (JniIO.get(b, o + 5) & 0xffL));
		case 6:
			off[0] += 7;
			return (long) (((h & 0x1fL) << 48L) | ((JniIO.get(b, o + 1) & 0xffL) << 40L)
					| ((JniIO.get(b, o + 2) & 0xffL) << 32L) | ((JniIO.get(b, o + 3) & 0xffL) << 24L)
					| ((JniIO.get(b, o + 4) & 0xffL) << 16L) | ((JniIO.get(b, o + 5) & 0xffL) << 8L)
					| (JniIO.get(b, o + 6) & 0xffL));
		case 7:
			off[0] += 8;
			return (long) (((h & 0x1fL) << 56L) | ((JniIO.get(b, o + 1) & 0xffL) << 48L)
					| ((JniIO.get(b, o + 2) & 0xffL) << 40L) | ((JniIO.get(b, o + 3) & 0xffL) << 32L)
					| ((JniIO.get(b, o + 4) & 0xffL) << 24L) | ((JniIO.get(b, o + 5) & 0xffL) << 16L)
					| ((JniIO.get(b, o + 6) & 0xffL) << 8L) | (JniIO.get(b, o + 7) & 0xffL));
		}
		throw new IllegalArgumentException("Invalid byte8Long condition:" + off[0]);
	}

	/**
	 * バイナリ文字変換.
	 * 
	 * @param b
	 *            対象のバイナリを設定します.
	 * @param pos
	 *            対象のポジションを設定します.
	 * @return String 対象の情報が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final String byteString(long b, int[] pos) throws Exception {
		int len = byte4Int(b, pos);
		if (len == 0) {
			return "";
		}
		String ret = JniIO.getUtf16(b, pos[0], len);
		pos[0] += len;
		return ret;
	}

	/**
	 * バイナリLevelArray変換.
	 * 
	 * @param b
	 *            対象のバイナリを設定します.
	 * @param pos
	 *            対象のポジションを設定します.
	 * @param length
	 *            対象の長さを設定します.
	 * @return LevelArray 対象の情報が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final LevelArray byteLevelArray(int[] pos, JniBuffer b, int length) throws Exception {
		LevelArray ret = new LevelArray();
		int len = byte4Int(b.address(), pos);
		if (len == 0) {
			return ret;
		}

		int i;
		Object[] keyList = new Object[len + LevelArray.ADD_LEN];
		Object[] dataList = new Object[len + LevelArray.ADD_LEN];
		for (i = 0; i < len; i++) {
			keyList[i] = decodeObject(pos, b, length);
			dataList[i] = decodeObject(pos, b, length);
		}
		ret.keyList = keyList;
		ret.dataList = dataList;
		ret.length = len;
		return ret;
	}

	/**
	 * バイナリTwoKey変換.
	 * 
	 * @param code
	 *            2キータイプを設定します.
	 * @param b
	 *            対象のバイナリを設定します.
	 * @param pos
	 *            対象のポジションを設定します.
	 * @return LevelArray 対象の情報が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final TwoKey byteTwoKey(int code, int[] pos, JniBuffer b) throws Exception {
		int len = byte4Int(b.address(), pos);
		TwoKey ret = null;
		switch (code) {
		case LevelOption.TYPE_STR_STR:
			ret = new StrStr(b, pos[0], len);
			break;
		case LevelOption.TYPE_STR_N32:
			ret = new StrInt(b, pos[0], len);
			break;
		case LevelOption.TYPE_STR_N64:
			ret = new StrLong(b, pos[0], len);
			break;
		case LevelOption.TYPE_N32_STR:
			ret = new IntStr(b, pos[0], len);
			break;
		case LevelOption.TYPE_N32_N32:
			ret = new IntInt(b, pos[0], len);
			break;
		case LevelOption.TYPE_N32_N64:
			ret = new IntLong(b, pos[0], len);
			break;
		case LevelOption.TYPE_N64_STR:
			ret = new LongStr(b, pos[0], len);
			break;
		case LevelOption.TYPE_N64_N32:
			ret = new LongInt(b, pos[0], len);
			break;
		case LevelOption.TYPE_N64_N64:
			ret = new LongLong(b, pos[0], len);
			break;
		case LevelOption.TYPE_STR_BIN:
			ret = new StrBin(b, pos[0], len);
			break;
		case LevelOption.TYPE_N32_BIN:
			ret = new IntBin(b, pos[0], len);
			break;
		case LevelOption.TYPE_N64_BIN:
			ret = new LongBin(b, pos[0], len);
			break;
		}
		if (ret != null) {
			pos[0] += len;
			return ret;
		}
		throw new IllegalArgumentException("The specified TwoKey condition is out of range:" + code);
	}

	/**
	 * シリアライズ変換.
	 * 
	 * @param b
	 *            対象のバイナリを設定します.
	 * @param pos
	 *            対象のポジションを設定します.
	 * @return Object 対象の情報が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final Object byteSerial(long b, int[] pos) throws Exception {
		int len = byte4Int(b, pos);
		if (len == 0) {
			return null;
		}
		byte[] bb = new byte[len];
		JniIO.getBinary(b, pos[0], bb, 0, len);
		Object ret = toObject(bb, 0, len);
		pos[0] += len;
		return ret;
	}

	/**
	 * バイナリをシリアライズオブジェクトに変換.
	 * 
	 * @param bin
	 *            対象のバイナリを設定します.
	 * @param off
	 *            対象のオフセット値を設定します.
	 * @param len
	 *            対象の長さを設定します.
	 * @return Serializable 変換されたシリアライズオブジェクトが返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final Serializable toObject(byte[] bin, int off, int len) throws Exception {
		if (bin == null || bin.length <= 0) {
			throw new IllegalArgumentException("Binary length for serialization restore does not exist.");
		}
		ObjectInputStream in = null;
		Serializable ret = null;
		try {
			in = new ObjectInputStream(new ByteArrayInputStream(bin, off, len));
			ret = (Serializable) in.readObject();
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				in.close();
			} catch (Exception e) {
			}
			in = null;
		}
		return ret;
	}

	/** オブジェクトを表すクラス. **/
	private static final Class OBJECT_CLASS = Object.class;

	/**
	 * オブジェクトデータ変換.
	 * 
	 * @param buf
	 *            対象のバッファを設定します.
	 * @param o
	 *            対象のオブジェクトを設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final void encodeObject(JniBuffer buf, Object o) throws Exception {
		byte[] b;
		
		// その他変換コードが設定されている場合.
		if(ORIGIN_CODE != null) {
			// オブジェクト変換.
			o = ORIGIN_CODE.inObject(o);
			
			// その他変換コードが設定されている場合.
			if(ORIGIN_CODE.encode(buf, o)) {
				return;
			}
		}
		if (o == null) {
			head(buf, 0xff); // null.
		} else if (o instanceof Number) {

			if (o instanceof Integer) {
				head(buf, 6); // Integer.
				byte4(buf, (Integer) o);
			} else if (o instanceof Long) {
				head(buf, 7); // Long.
				byte8(buf, (Long) o);
			} else if (o instanceof Double) {
				head(buf, 9); // Double.
				byte8(buf, Double.doubleToRawLongBits((Double) o));
			} else if (o instanceof Float) {
				head(buf, 8); // Float.
				byte4(buf, Float.floatToRawIntBits((Float) o));
			} else if (o instanceof Byte) {
				head(buf, 4); // byte.
				byte1(buf, (Byte) o);
			} else if (o instanceof Short) {
				head(buf, 5); // Short.
				byte2(buf, (Short) o);
			} else if (o instanceof AtomicInteger) {
				head(buf, 10); // AtomicInteger.
				byte4(buf, ((AtomicInteger) o).get());
			} else if (o instanceof AtomicLong) {
				head(buf, 11); // AtomicLong.
				byte8(buf, ((AtomicLong) o).get());
			} else if (o instanceof BigDecimal) {
				head(buf, 12); // BigDecimal.
				// 文字変換.
				stringBinary(buf, o.toString());
			} else if (o instanceof BigInteger) {
				head(buf, 13); // BigInteger.
				// 文字変換.
				stringBinary(buf, o.toString());
			}
		} else if (o instanceof String) {
			head(buf, 1); // string.
			stringBinary(buf, (String) o);
		} else if (o instanceof Boolean) {
			head(buf, 2); // boolean.
			byte1(buf, ((Boolean) o).booleanValue() ? 1 : 0);
		} else if (o instanceof LevelArray) {

			// LevelArrayオブジェクト.
			head(buf, 70);
			levelArrayBinary(buf, (LevelArray) o);
		} else if (o instanceof TwoKey) {

			// 2キー情報.
			head(buf, 80);

			// キータイプをセット.
			byte1(buf, ((TwoKey) o).getType());

			// 2キー情報を変換.
			twoKeyBinary(buf, (TwoKey) o);

		} else if (o.getClass().isArray()) {

			if (o instanceof long[]) {
				head(buf, 25); // long配列.
				long[] c = (long[]) o;
				int len = c.length;
				byte4(buf, len); // 長さ.
				for (int i = 0; i < len; i++) {
					byte8(buf, c[i]);
				}
			} else if (o instanceof int[]) {
				head(buf, 24); // int配列.
				int[] c = (int[]) o;
				int len = c.length;
				byte4(buf, len); // 長さ.
				for (int i = 0; i < len; i++) {
					byte4(buf, c[i]);
				}
			} else if (o instanceof String[]) {
				head(buf, 28); // String配列.
				String[] c = (String[]) o;
				int len = c.length;
				byte4(buf, len); // 長さ.
				for (int i = 0; i < len; i++) {
					stringBinary(buf, c[i]);
				}
			} else if (o instanceof byte[]) {
				head(buf, 21); // byte配列.
				b = (byte[]) o;
				byte4(buf, b.length); // 長さ.
				buf.write(b, 0, b.length); // body.
				b = null;
			} else if (o instanceof double[]) {
				head(buf, 27); // double配列.
				double[] c = (double[]) o;
				int len = c.length;
				byte4(buf, len); // 長さ.
				for (int i = 0; i < len; i++) {
					byte8(buf, Double.doubleToRawLongBits(c[i]));
				}
			} else if (o instanceof float[]) {
				head(buf, 26); // float配列.
				float[] c = (float[]) o;
				int len = c.length;
				byte4(buf, len); // 長さ.
				for (int i = 0; i < len; i++) {
					byte4(buf, Float.floatToRawIntBits(c[i]));
				}
			} else if (o instanceof boolean[]) {
				head(buf, 20); // boolean配列.
				boolean[] c = (boolean[]) o;
				int len = c.length;
				byte4(buf, len); // 長さ.
				for (int i = 0; i < len; i++) {
					byte1(buf, c[i] ? 1 : 0);
				}
			} else if (o instanceof Object[]) {
				head(buf, 50); // 他配列.
				byte4(buf, 0); // Object配列.
				Object[] c = (Object[]) o;
				int len = c.length;
				byte4(buf, len); // 長さ.
				for (int i = 0; i < len; i++) {
					encodeObject(buf, c[i]);
				}
			} else if (o instanceof char[]) {
				head(buf, 22); // char配列.
				char[] c = (char[]) o;
				int len = c.length;
				byte4(buf, len); // 長さ.
				for (int i = 0; i < len; i++) {
					byte2(buf, c[i]);
				}
			} else if (o instanceof short[]) {
				head(buf, 23); // short配列.
				short[] c = (short[]) o;
				int len = c.length;
				byte4(buf, len); // 長さ.
				for (int i = 0; i < len; i++) {
					byte2(buf, c[i]);
				}
			} else {

				// 配列のオブジェクトを取得.
				final Class c = o.getClass().getComponentType();

				// 1次配列オブジェクトの場合.
				if (c != null && !c.isArray()) {

					// 他配列.
					head(buf, 50); // 他配列.

					// java.lang.Object配列の場合は、1バイトゼロ数をセット.
					if (OBJECT_CLASS.equals(c)) {
						byte4(buf, 0);
					} else {
						stringBinary(buf, c.getName());
					}
					int len = Array.getLength(o);

					byte4(buf, len); // 長さ.
					for (int i = 0; i < len; i++) {
						encodeObject(buf, Array.get(o, i));
					}
				}
				// 多重配列の場合.
				// 多重配列はサポート外.
				else {

					// nullをセット.
					head(buf, 0xff); // null.
				}
			}
		} else if (o instanceof java.util.Date) {
			head(buf, 14); // Date.
			if (o instanceof java.sql.Date) {
				byte1(buf, 1);
			} else if (o instanceof java.sql.Time) {
				byte1(buf, 2);
			} else if (o instanceof java.sql.Timestamp) {
				byte1(buf, 3);
			}
			// 他日付オブジェクトの場合.
			else {
				byte1(buf, 4);
			}
			byte8(buf, ((Date) o).getTime());
		} else if (o instanceof List) {
			head(buf, 51); // Listオブジェクト.
			List lst = (List) o;
			int len = lst.size();
			byte4(buf, len); // 長さ.
			for (int i = 0; i < len; i++) {
				encodeObject(buf, lst.get(i));
			}
		} else if (o instanceof Map) {
			head(buf, 52); // Mapオブジェクト.
			Object k;
			Map map = (Map) o;
			byte4(buf, map.size()); // 長さ.
			Iterator it = map.keySet().iterator();
			while (it.hasNext()) {
				k = it.next();
				encodeObject(buf, k); // キー.
				encodeObject(buf, map.get(k)); // 要素.
			}
		} else if (o instanceof Set) {
			head(buf, 53); // Setオブジェクト.
			Set set = (Set) o;
			byte4(buf, set.size()); // 長さ.
			Iterator it = set.iterator();
			while (it.hasNext()) {
				encodeObject(buf, it.next()); // キー.
			}
		} else if (o instanceof Character) {
			head(buf, 3); // char.
			byte2(buf, (Character) o);
		} else if (o instanceof Serializable) {

			// シリアライズオブジェクト.
			head(buf, 60);
			serialBinary(buf, (Serializable) o);
		} else {
			// それ以外のオブジェクトは変換しない.
			head(buf, 0xff); // null.
		}
	}
	
	/**
	 * オブジェクト解析.
	 * 
	 * @param pos
	 *            対象のポジションを設定します.
	 * @param b
	 *            対象のバイナリを設定します.
	 * @param length
	 *            対象の長さを設定します.
	 * @return Object 変換されたオブジェクトが返却されます.
	 */
	public static final Object decodeObject(int[] pos, JniBuffer b, int length) throws Exception {
		Object ret = _decodeObject(pos, b, length);
		if(ORIGIN_CODE != null) {
			return ORIGIN_CODE.outObject(ret);
		}
		return ret;
	}
	
	// オブジェクト解析.
	private static final Object _decodeObject(int[] pos, JniBuffer b, int length) throws Exception {
		if (length <= pos[0]) {
			throw new IOException("Processing exceeds the specified length [" + length + " byte]:" + pos[0]);
		}

		long addr = b.address();
		int i, len;
		Object ret;
		int code = head(addr, pos);
		switch (code) {
		case 1: {
			// string.
			return byteString(addr, pos);
		}
		case 2: {
			// boolean.
			ret = (byte1Int(addr, pos) == 1);
			return ret;
		}
		case 3: {
			// char.
			ret = (char) byte2Int(addr, pos);
			return ret;
		}
		case 4: {
			// byte.
			ret = (byte) byte1Int(addr, pos);
			return ret;
		}
		case 5: {
			// short.
			ret = (short) byte2Int(addr, pos);
			return ret;
		}
		case 6: {
			// int.
			ret = byte4Int(addr, pos);
			return ret;
		}
		case 7: {
			// long.
			ret = byte8Long(addr, pos);
			return ret;
		}
		case 8: {
			// float.
			ret = Float.intBitsToFloat(byte4Int(addr, pos));
			return ret;
		}
		case 9: {
			// double.
			ret = Double.longBitsToDouble(byte8Long(addr, pos));
			return ret;
		}
		case 10: {
			// AtomicInteger.
			ret = new AtomicInteger(byte4Int(addr, pos));
			return ret;
		}
		case 11: {
			// AtomicLong.
			ret = new AtomicLong(byte8Long(addr, pos));
			return ret;
		}
		case 12: {
			// BigDecimal.
			return new BigDecimal(byteString(addr, pos));
		}
		case 13: {
			// BigInteger.
			return new BigInteger(byteString(addr, pos));
		}
		case 14: {
			// Date.
			int type = byte1Int(addr, pos);
			if (type == 1) {
				ret = new java.sql.Date(byte8Long(addr, pos));
			} else if (type == 2) {
				ret = new java.sql.Time(byte8Long(addr, pos));
			} else if (type == 3) {
				ret = new java.sql.Timestamp(byte8Long(addr, pos));
			} else {
				ret = new java.util.Date(byte8Long(addr, pos));
			}
			return ret;
		}
		case 20: {
			// boolean配列.
			len = byte4Int(addr, pos);
			boolean[] lst = new boolean[len];
			for (i = 0; i < len; i++) {
				lst[i] = (byte1Int(addr, pos) == 1);
			}
			return lst;
		}
		case 21: {
			// byte配列.
			len = byte4Int(addr, pos);
			byte[] lst = new byte[len];
			JniIO.getBinary(addr, pos[0], lst, 0, len);
			pos[0] += len;
			return lst;
		}
		case 22: {
			// char配列.
			len = byte4Int(addr, pos);
			char[] lst = new char[len];
			for (i = 0; i < len; i++) {
				lst[i] = (char) byte2Int(addr, pos);
			}
			return lst;
		}
		case 23: {
			// short配列.
			len = byte4Int(addr, pos);
			short[] lst = new short[len];
			for (i = 0; i < len; i++) {
				lst[i] = (short) byte2Int(addr, pos);
			}
			return lst;
		}
		case 24: {
			// int配列.
			len = byte4Int(addr, pos);
			int[] lst = new int[len];
			for (i = 0; i < len; i++) {
				lst[i] = byte4Int(addr, pos);
			}
			return lst;
		}
		case 25: {
			// long配列.
			len = byte4Int(addr, pos);
			long[] lst = new long[len];
			for (i = 0; i < len; i++) {
				lst[i] = byte8Long(addr, pos);
			}
			return lst;
		}
		case 26: {
			// float配列.
			len = byte4Int(addr, pos);
			float[] lst = new float[len];
			for (i = 0; i < len; i++) {
				lst[i] = Float.intBitsToFloat(byte4Int(addr, pos));
			}
			return lst;
		}
		case 27: {
			// double配列.
			len = byte4Int(addr, pos);
			double[] lst = new double[len];
			for (i = 0; i < len; i++) {
				lst[i] = Double.longBitsToDouble(byte8Long(addr, pos));
			}
			return lst;
		}
		case 28: {
			// String配列.
			len = byte4Int(addr, pos);
			String[] lst = new String[len];
			for (i = 0; i < len; i++) {
				lst[i] = byteString(addr, pos);
			}
			return lst;
		}
		case 50: {
			// 配列.
			String cls = byteString(addr, pos);
			len = byte4Int(addr, pos);
			Object lst;
			// 情報が存在しない場合は、Object配列.
			if (cls.length() == 0) {
				Object[] n = new Object[len];
				for (i = 0; i < len; i++) {
					n[i] = decodeObject(pos, b, length);
				}
				lst = n;
			} else {
				lst = Array.newInstance(Class.forName(cls), len);
				for (i = 0; i < len; i++) {
					Array.set(lst, i, decodeObject(pos, b, length));
				}
			}
			return lst;
		}
		case 51: {
			// List.
			len = byte4Int(addr, pos);
			List lst = new ObjectList(len);
			for (i = 0; i < len; i++) {
				lst.add(decodeObject(pos, b, length));
			}
			return lst;
		}
		case 52: {
			// Map.
			len = byte4Int(addr, pos);
			Map map = new ArrayMap();
			for (i = 0; i < len; i++) {
				map.put(decodeObject(pos, b, length), decodeObject(pos, b, length));
			}
			return map;
		}
		case 53: {
			// Set.
			Set set = new HashSet();
			len = byte4Int(addr, pos);
			for (i = 0; i < len; i++) {
				set.add(decodeObject(pos, b, length));
			}
			return set;
		}
		case 60: {
			// シリアライズ可能オブジェクト.
			return byteSerial(addr, pos);
		}
		case 70: {
			// LevelArray変換.
			return byteLevelArray(pos, b, length);
		}
		case 80: {
			// TwoKey変換.
			return byteTwoKey(byte1Int(addr, pos), pos, b);
		}
		case 0xff: {
			// NULL.
			return null;
		}
		
		}
		// その他変換コードが設定されている場合.
		if(ORIGIN_CODE != null && code >= OriginCode.USE_OBJECT_CODE) {
			ret = ORIGIN_CODE.decode(pos, code, b, length);
			if(ret != null) {
				return ret;
			}
			ORIGIN_CODE.noneDecode(code);
		}
		throw new IOException("Unknown type '" + code + "' detected.");
	}
	
	/**
	 * 有効最大ビット長を取得.
	 * 
	 * @param x
	 *            対象の数値を設定します.
	 * @return int 左ゼロビット数が返却されます.
	 */
	public static final int nlzs(int x) {
		if (x == 0) {
			return 0;
		}
		x |= (x >> 1);
		x |= (x >> 2);
		x |= (x >> 4);
		x |= (x >> 8);
		x |= (x >> 16);
		x = (x & 0x55555555) + (x >> 1 & 0x55555555);
		x = (x & 0x33333333) + (x >> 2 & 0x33333333);
		x = (x & 0x0f0f0f0f) + (x >> 4 & 0x0f0f0f0f);
		x = (x & 0x00ff00ff) + (x >> 8 & 0x00ff00ff);
		return (x & 0x0000ffff) + (x >> 16 & 0x0000ffff);
	}

	/**
	 * 有効最大ビット長を取得.
	 * 
	 * @param x
	 *            対象の数値を設定します.
	 * @return int 左ゼロビット数が返却されます.
	 */
	public static final int nlzs(long x) {
		final int xx = (int) ((x & 0xffffffff00000000L) >> 32L);
		if (nlzs(xx) == 0) {
			return nlzs((int) (x & 0x00000000ffffffff));
		}
		return nlzs(xx) + 32;
	}

	/**
	 * オブジェクト配列をエンコード.
	 * 
	 * @params buf 出力先のバッファ先を設定します.
	 * @param c オブジェク配列を設定します.
	 * @exception Exception 例外.
	 */
//	public static final void encodeObjectArray(JniBuffer buf, Object... c) throws Exception {
//		head(buf, 50); // 他配列.
//		byte4(buf, 0); // Object配列.
//		int len = c == null ? 0 : c.length;
//		byte4(buf, len); // 長さ.
//		for (int i = 0; i < len; i++) {
//			encodeObject(buf, c[i]);
//		}
//	}
	
	/**
	 * オブジェクト配列をデコード.
	 * @param buf
	 * @param off
	 * @return
	 * @throws Exception
	 */
//	public static final Object decodeObjectArray(int[] off, JniBuffer buf) throws Exception {
//		long b = buf.address();
//		int length = buf.position();
//		int code = byte1Int(b, off);
//		if(code != 50) {
//			throw new LeveldbException("Object array conversion pattern does not match.");
//		}
//		code = byte4Int(b, off);
//		if(code != 0) {
//			throw new LeveldbException("Object array conversion pattern does not match.");
//		}
//		int len = byte4Int(b, off);
//		Object[] ret = new Object[len];
//		for(int i = 0; i < len; i ++) {
//			ret[i] = decodeObject(off, buf, length);
//		}
//		return ret;
//	}

	/**
	 * 拡張エンコード、デコード処理を行う場合の継承クラス.
	 * 
	 * エンコード時には、必ず
	 * 
	 *  LevelValues.head(buf, USE_OBJECT_CODE); // objectCode(100番以降をセット).
	 *  オブジェクトを変換.
	 *  
	 * のように設定します.
	 */
	public static abstract class OriginCode {
		/**
		 * オブジェクトコード利用可能開始番号.
		 */
		protected static final int USE_OBJECT_CODE = 100;
		
		/**
		 * 入力オブジェクトの変換.
		 * LevelValues.encodeObject で処理される毎に、この処理が呼ばれます.
		 * 
		 * @param o オブジェクトを設定します.
		 * @return Object 変換されたオブジェクトが返却されます.
		 * @exception Exception 例外.
		 */
		public Object inObject(Object o) throws Exception {
			return o;
		}
		
		/**
		 * 出力オブジェクトの変換.
		 * LevelValues.decodeObject で処理結果毎に、この処理が呼ばれます.
		 * 
		 * @param o オブジェクトを設定します.
		 * @return Object 変換されたオブジェクトが返却されます.
		 * @exception Exception 例外.
		 */
		public Object outObject(Object o) throws Exception {
			return o;
		}
		
		/**
		 * オブジェクトデータ変換.
		 * 
		 * @param buf
		 *            対象のバッファを設定します.
		 * @param o
		 *            対象のオブジェクトを設定します.
		 * @return boolean
		 *            変換出来た場合は[true]を返却します.
		 * @exception Exception
		 *                例外.
		 */
		public abstract boolean encode(JniBuffer buf, Object o) throws Exception;
		
		/**
		 * オブジェクト解析.
		 * 
		 * @param pos
		 *            対象のポジションを設定します.
		 * @param objectCode
		 *            オブジェクトコードが設定されます.
		 * @param b
		 *            対象のバイナリを設定します.
		 * @param length
		 *            対象の長さを設定します.
		 * @return Object 変換されたオブジェクトが返却されます.
		 */
		public abstract Object decode(int[] pos, int objectCode, JniBuffer b, int length) throws Exception;
		
		/**
		 * 当てはまらない条件のデコード返却.
		 * デコード対象のオブジェクトコードの場合は、この処理を呼び出します.
		 * 
		 * @param objectCode
		 */
		public void noneDecode(int objectCode) throws Exception {
			throw new IOException("Unknown type '" + objectCode + "' detected.");
		}
	}
}
