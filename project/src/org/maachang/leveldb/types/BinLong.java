package org.maachang.leveldb.types;

import java.util.Arrays;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.JniIO;
import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.util.BinaryUtil;
import org.maachang.leveldb.util.Converter;

/**
 * バイナリ、Number64(long)の２キー情報.
 */
public final class BinLong extends TwoKey {

	/** １つ目の文字キー. **/
	private byte[] one = NONE;

	/** 2つ目のキー. **/
	private Long two = 0L;

	/**
	 * コンストラクタ.
	 */
	public BinLong() {

	}

	/**
	 * コンストラクタ.
	 * 
	 * @param one
	 *            1つ目のキーを設定します.
	 */
	public BinLong(Object one) {
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
	public BinLong(Object one, Object two) {
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
	public BinLong(JniBuffer buf) throws Exception {
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
	public BinLong(JniBuffer buf, int off, int len) throws Exception {
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
		long addr = buf.address() + off;

		// データ長の位置にセット.
		len -= 4;

		// one.
		int oneLen = (int) (JniIO.getIntE(addr, len) & 0x7fffffff);
		if (oneLen == 0) {
			one = NONE;
		} else {
			one = new byte[oneLen];
			JniIO.getBinary(addr, 0, one, 0, oneLen);
		}

		// two.
		two = JniIO.getLongE(addr, oneLen);
	}

	/**
	 * toBuffer時のバイナリ長を取得.
	 * 
	 * @return int バイナリ長が返却されます.
	 */
	public final int toBufferLength() {
		return one.length + 12;
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
	public static final void convertBuffer(Object one, Object two, JniBuffer buf) throws Exception {
		int pos = buf.position();

		// それぞれの長さを取得.
		int len = ((byte[]) one).length;
		long addr = buf.recreate(true, pos + len + 12);

		// one.
		if (len != 0) {
			JniIO.putBinary(addr, pos, (byte[]) one, 0, len);
		}

		// two.
		JniIO.putLong(addr, pos + len, (Long) two);
		// oneとtwoの後に４バイトでoneのデータ長をセット.
		JniIO.putInt(addr, pos + len + 8, len);
		buf.addPosition(len + 12);
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
		if (o == null || !Converter.isNumeric(o)) {
			this.two = 0L;
		} else {
			this.two = Converter.convertLong(o);
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
		return LevelOption.TYPE_BIN_N64;
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
		if (o instanceof BinLong) {
			TwoKey t = (TwoKey) o;
			int ret = BinaryUtil.binaryCompareTo(one, (byte[]) t.one());
			if (ret == 0) {

				// 数値の場合は、数値同士で比較.
				if (Converter.isNumeric(t.two())) {
					return two.compareTo(Converter.convertLong(t.two()));
				}
				// 比較のTwoが数字でない場合は、大きいものとして扱う.
				return -1;
			}
			return ret;
		} else if (o instanceof byte[]) {

			// １つの文字オブジェクトとone文字との比較.
			int ret = BinaryUtil.binaryCompareTo(one, (byte[]) o);
			if (ret == 0) {
				return (two != 0) ? 1 : 0;
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
		return Arrays.hashCode(one) + two.hashCode();
	}

	/**
	 * 文字列として出力.
	 * 
	 * @return String 文字列が返却されます.
	 */
	public final String toString() {
		return new StringBuilder("[bin-num32]")
				.append(BinaryUtil.binaryToHexString(one))
				.append(" : ")
				.append(two)
				.toString();
	}
}
