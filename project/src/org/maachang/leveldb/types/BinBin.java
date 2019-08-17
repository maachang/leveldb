package org.maachang.leveldb.types;

import java.util.Arrays;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.JniIO;
import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.util.Utils;

/**
 * バイナリ、バイナリの２キー情報.
 */
public final class BinBin extends TwoKey {

	/** １つ目の文字キー. **/
	private byte[] one = NONE;

	/** 2つ目のキー. **/
	private byte[] two = NONE;

	/**
	 * コンストラクタ.
	 */
	public BinBin() {

	}

	/**
	 * コンストラクタ.
	 * 
	 * @param one
	 *            1つ目のキーを設定します.
	 */
	public BinBin(Object one) {
		this.set(one);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param one
	 *            1つ目のキーを設定します.
	 * @param two
	 *            2つ目のキーを設定します.
	 */
	public BinBin(Object one, Object two) {
		this.set(one, two);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param buf
	 *            対象のJniBufferを設定します.
	 * @exception Exception
	 *                例外.
	 */
	public BinBin(JniBuffer buf) throws Exception {
		create(buf);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param buf
	 *            対象のJniBufferを設定します.
	 * @param off
	 *            対象のオフセット値を設定します.
	 * @param len
	 *            対象の長さを設定します.
	 * @exception Exception.
	 */
	public BinBin(JniBuffer buf, int off, int len) throws Exception {
		create(buf, off, len);
	}

	/**
	 * JniBufferから情報を生成.
	 * 
	 * @param buf
	 *            対象のJniBufferを設定します.
	 * @param off
	 *            対象のオフセット値を設定します.
	 * @param len
	 *            対象の長さを設定します.
	 * @exception Exception.
	 */
	public final void create(JniBuffer buf, int off, int len) throws Exception {
		long addr = buf.address + off;

		// one.
		int oneLen = (int) (JniIO.getShortE(addr, 0) & 0x0000ffff);
		if (oneLen == 0) {
			one = NONE;
		} else {
			one = new byte[oneLen];
			JniIO.getBinary(addr, 2, one, 0, oneLen);
		}

		// two.
		if (len <= oneLen + 2) {
			two = NONE;
		} else {
			len -= (oneLen + 2);
			two = new byte[len];
			JniIO.getBinary(addr, 2 + oneLen, two, 0, len);
		}
	}

	/**
	 * toBuffer時のバイナリ長を取得.
	 * 
	 * @return int バイナリ長が返却されます.
	 */
	public final int toBufferLength() {
		return one.length + +two.length + 2;
	}

	/**
	 * 格納されたキー情報をJniBufferに変換.
	 * 
	 * @param buf
	 *            対象のJniBufferを設定します.
	 * @exception Exception.
	 */
	public final void toBuffer(JniBuffer buf) throws Exception {
		convertBuffer(one, two, buf);
	}

	/**
	 * 格納されたキー情報をJniBufferに変換.
	 * 
	 * @param one
	 *            1番目のキーを設定します.
	 * @parma two 2番目のキーを設定します.
	 * @param buf
	 *            対象のJniBufferを設定します.
	 * @exception Exception.
	 */
	public static final void convertBuffer(Object one, Object two, JniBuffer buf)
			throws Exception {
		int pos = buf.position;

		// それぞれの長さを取得.
		int len = ((byte[]) one).length;
		int len2 = ((byte[]) two).length;
		long addr = buf.recreate(true, pos + len + len2 + 2);

		// one.
		JniIO.putShort(addr, pos, (short) len);
		if (len != 0) {
			JniIO.putBinary(addr, pos + 2, (byte[]) one, 0, len);
		}

		// two.
		if (len2 != 0) {
			JniIO.putBinary(addr, pos + len + 2, (byte[]) two, 0, len2);
		}
		buf.position += len + len2 + 2;
	}

	/**
	 * １番目のキーを設定.
	 * 
	 * @param o
	 *            設定情報を設定します.
	 * @return TwoKey オブジェクトが返却されます.
	 */
	public final TwoKey one(Object o) {
		if (o == null) {
			this.one = NONE;
		} else if (o instanceof byte[]) {
			this.one = (byte[]) o;
		} else {
			throw new LeveldbException("指定パラメータはバイナリではありません");
		}
		return this;
	}

	/**
	 * ２番目のキーを設定.
	 * 
	 * @param o
	 *            設定情報を設定します.
	 * @return TwoKey オブジェクトが返却されます.
	 */
	public final TwoKey two(Object o) {
		if (o == null) {
			this.two = NONE;
		} else if (o instanceof byte[]) {
			this.two = (byte[]) o;
		} else {
			throw new LeveldbException("指定パラメータはバイナリではありません");
		}
		return this;
	}

	/**
	 * １番目のキーを取得.
	 * 
	 * @return Object １番目のキーが返却されます.
	 */
	public final Object one() {
		return one;
	}

	/**
	 * ２番目のキーを取得.
	 * 
	 * @return Object ２番目のキーが返却されます.
	 */
	public final Object two() {
		return two;
	}

	/**
	 * キータイプを取得.
	 * 
	 * @return int キータイプが返却されます.
	 */
	public final int getType() {
		return LevelOption.TYPE_BIN_BIN;
	}

	/**
	 * 判別処理.
	 * 
	 * @param o
	 *            対象のオブジェクトを設定します.
	 * @return int 判別結果が返却されます. このオブジェクトが指定されたオブジェクトより小さい場合は負の整数、
	 *         等しい場合はゼロ、大きい場合は正の整数
	 */
	public final int compareTo(Object o) {
		if (o == this)
			return 0;

		// データがTwoKeyの場合は、２つの条件を文字文字でチェック.
		if (o instanceof BinBin) {
			TwoKey t = (TwoKey) o;
			int ret = Utils.binaryCompareTo(one, (byte[]) t.one());
			if (ret == 0) {
				return Utils.binaryCompareTo(two, (byte[]) t.two());
			}
			return ret;
		} else if (o instanceof byte[]) {

			// １つの文字オブジェクトとone文字との比較.
			int ret = Utils.binaryCompareTo(one, (byte[]) o);
			if (ret == 0) {
				return (two.length > 0) ? 1 : 0;
			}
			return ret;
		}
		throw new LeveldbException("比較対象のオブジェクトは、キーがバイナリではありません");
	}

	/**
	 * 一致チェック.
	 * 
	 * @param o
	 *            対象のオブジェクトを設定します.
	 * @return boolean [true]の場合、完全一致しています.
	 */
	public final boolean equals(Object o) {
		return compareTo(o) == 0;
	}

	/**
	 * hash生成.
	 * 
	 * @return int ハッシュ情報が返却されます.
	 */
	public final int hashCode() {
		return Arrays.hashCode(one) + Arrays.hashCode(two);
	}

	/**
	 * 文字列として出力.
	 * 
	 * @return String 文字列が返却されます.
	 */
	public final String toString() {
		return new StringBuilder("[bin-bin]").append(
				Utils.binaryToHexString(one)).append(
				Utils.binaryToHexString(two)).toString();
	}

	/**
	 * JniBuffer出力.
	 * 
	 * @param buf
	 *            対象のJniBufferを設定します.
	 * @exception Exception
	 *                例外.
	 */
	public final void out(JniBuffer buf) throws Exception {
		toBuffer(buf);
	}

}
