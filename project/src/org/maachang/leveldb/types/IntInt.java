package org.maachang.leveldb.types;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.JniIO;
import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.util.Converter;

/**
 * Number32(int)、Number32(int)の２キー情報.
 */
public final class IntInt extends TwoKey {

	/** １つ目の文字キー. **/
	private Integer one = 0;

	/** 2つ目のキー. **/
	private Integer two = 0;

	/**
	 * コンストラクタ.
	 */
	public IntInt() {

	}

	/**
	 * コンストラクタ.
	 * 
	 * @param one
	 *            1つ目のキーを設定します.
	 */
	public IntInt(Object one) {
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
	public IntInt(Object one, Object two) {
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
	public IntInt(JniBuffer buf) throws Exception {
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
	public IntInt(JniBuffer buf, int off, int len) throws Exception {
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

		// one.
		one = JniIO.getIntE(addr, 0);

		// two.
		two = JniIO.getIntE(addr, 4);
	}

	/**
	 * toBuffer時のバイナリ長を取得.
	 * 
	 * @return int バイナリ長が返却されます.
	 */
	public final int toBufferLength() {
		return 8;
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
		long addr = buf.recreate(true, pos + 8);

		// one.
		JniIO.putInt(addr, pos, (Integer) one);

		// two.
		JniIO.putInt(addr, pos + 4, (Integer) two);
		buf.addPosition(8);
	}

	/**
	 * １番目のキーを設定.
	 * 
	 * @param o
	 *            設定情報を設定します.
	 * @return TwoKey オブジェクトが返却されます.
	 */
	public final TwoKey one(Object o) {
		if (o == null || !Converter.isNumeric(o)) {
			this.one = 0;
		} else {
			this.one = Converter.convertInt(o);
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
			this.two = 0;
		} else {
			this.two = Converter.convertInt(o);
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
		return LevelOption.TYPE_N32_N32;
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

			// oneが数字の場合.
			if (Converter.isNumeric(t.one())) {
				int ret = one.compareTo(Converter.convertInt(t.one()));
				if (ret == 0) {
					if (Converter.isNumeric(t.two())) {
						return two.compareTo(Converter.convertInt(t.two()));
					}
					// 比較のoneが数字でない場合は、大きいものとして扱う.
					return -1;
				}
				return ret;
			}
			// 比較のoneが数字でない場合は、大きいものとして扱う.
			return -1;
		}
		// オブジェクトがNULLか、数字の場合.
		if (o == null || Converter.isNumeric(o)) {

			// 数字変換.
			o = Converter.convertInt(o);
			if (o == null) {
				o = 0;
			}
			// １つの文字オブジェクトとone文字との比較.
			int ret = one.compareTo((Integer) o);
			if (ret == 0) {
				return two.compareTo(0);
			}
			return ret;
		}
		// 比較のoneが数字でない場合は、大きいものとして扱う.
		return -1;
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
		return one.hashCode() + two.hashCode();
	}

	/**
	 * 文字列として出力.
	 * 
	 * @return String 文字列が返却されます.
	 */
	public final String toString() {
		return new StringBuilder("[num32-num32]")
				.append(one)
				.append(" : ")
				.append(two)
				.toString();
	}
}
