package org.maachang.leveldb.types;

import java.util.Arrays;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.JniIO;
import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.NativeString;
import org.maachang.leveldb.util.BinaryUtil;
import org.maachang.leveldb.util.Converter;

/**
 * 文字列、バイナリの２キー情報.
 */
public final class StrBin extends TwoKey {

	/** １つ目の文字キー. **/
	private String one = "";

	/** 2つ目のキー. **/
	private byte[] two = NONE;

	/**
	 * コンストラクタ.
	 */
	public StrBin() {

	}

	/**
	 * コンストラクタ.
	 * 
	 * @param one
	 *            1つ目のキーを設定します.
	 */
	public StrBin(Object one) {
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
	public StrBin(Object one, Object two) {
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
	public StrBin(JniBuffer buf) throws Exception {
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
	public StrBin(JniBuffer buf, int off, int len) throws Exception {
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
			one = "";
		} else {
			one = NativeString.toJava(addr, 0, oneLen);
		}

		// two.
		if (len <= oneLen) {
			two = NONE;
		} else {
			len -= oneLen;
			two = new byte[len];
			JniIO.getBinary(addr, oneLen, two, 0, len);
		}
	}

	/**
	 * toBuffer時のバイナリ長を取得.
	 * 
	 * @return int バイナリ長が返却されます.
	 */
	public final int toBufferLength() {
		return NativeString.nativeLength(one) + two.length + 4;
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
		int len = NativeString.nativeLength((String) one);
		int len2 = ((byte[]) two).length;
		long addr = buf.recreate(true, pos + len + len2 + 4);

		// one.
		if(len != 0) {
			NativeString.toNative(addr, pos, (String) one);
		}

		// two.
		if (len2 != 0) {
			JniIO.putBinary(addr, pos + len, (byte[]) two, 0, len2);
		}
		// oneとtwoの後に４バイトでoneのデータ長をセット.
		JniIO.putInt(addr, pos + len + len2, len);
		buf.addPosition(len + len2 + 4);
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
			this.one = "";
		} else {
			this.one = Converter.convertString(o);
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
		return LevelOption.TYPE_STR_BIN;
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
		if (o instanceof TwoKey) {
			TwoKey t = (TwoKey) o;
			int ret = one.compareTo(Converter.convertString(t.one()));
			if (ret == 0) {
				if (t.two() instanceof byte[]) {
					return BinaryUtil.binaryCompareTo(two, (byte[]) t.two());
				}
			}
			return ret;
		}
		// それ以外の場合は、文字オブジェクトとして変換.
		if ((Converter.convertString(o)) == null) {
			o = "";
		}
		// １つの文字オブジェクトとone文字との比較.
		int ret = one.compareTo((String) o);
		if (ret == 0) {
			return (two.length > 0) ? 1 : 0;
		}
		return ret;
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
		return one.hashCode() + Arrays.hashCode(two);
	}

	/**
	 * 文字列として出力.
	 * 
	 * @return String 文字列が返却されます.
	 */
	public final String toString() {
		return new StringBuilder("[str-bin]")
			.append(one)
			.append(" : ")
			.append(BinaryUtil.binaryToHexString(two))
			.toString();
	}

}
