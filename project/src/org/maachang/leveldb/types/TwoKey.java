package org.maachang.leveldb.types;

import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.List;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.LeveldbException;

/**
 * ２つのキーを保持する情報. Leveldbでは、１つのキーしか管理できないので、
 * この情報では、１つのキーで２つのキーを扱うような振舞をするような仕組みを取る.
 */
public abstract class TwoKey extends AbstractList<Object> implements LevelKey<Object> {

	/** NONE-BINARY. **/
	public static final byte[] NONE = new byte[0];

	/**
	 * JniBufferから情報を生成.
	 * 
	 * @param buf
	 *            対象のJniBufferを設定します.
	 * @exception Exception.
	 */
	public final void create(JniBuffer buf) throws Exception {
		create(buf, 0, buf.position());
	}

	/**
	 * JniBufferから情報を生成.
	 * 
	 * @param buf
	 *            対象のJniBufferを設定します.
	 * @param off
	 *            対象のオフセット値を設定します.
	 * @param len
	 *            対象のデータ長を設定します.
	 * @exception Exception.
	 */
	public abstract void create(JniBuffer buf, int off, int len) throws Exception;

	/**
	 * toBuffer時のバイナリ長を取得.
	 * 
	 * @return int バイナリ長が返却されます.
	 */
	public abstract int toBufferLength();

	/**
	 * 格納されたキー情報をJniBufferに変換.
	 * 
	 * @param buf
	 *            対象のJniBufferを設定します.
	 * @exception Exception.
	 */
	public abstract void toBuffer(JniBuffer buf) throws Exception;

	/**
	 * キーセット.
	 * 
	 * @param value
	 *            条件を設定します.
	 * @return TwoKey オブジェクトが返却されます.
	 */
	@SuppressWarnings("rawtypes")
	public final TwoKey set(Object value) {

		// NULLの場合.
		if (value == null) {
			one(null);
			two(null);
		}
		// TwoKeyオブジェクトの場合.
		else if (value instanceof TwoKey) {
			TwoKey t = (TwoKey) value;
			one(t.one());
			two(t.two());
		}
		// 配列の場合.
		else if (value.getClass().isArray()) {
			switch (Array.getLength(value)) {
			case 0:
				one(null);
				two(null);
				return this;
			case 1:
				one(Array.get(value, 0));
				two(null);
				return this;
			}
			one(Array.get(value, 0));
			two(Array.get(value, 1));
		}
		// リストの場合.
		else if (value instanceof List) {
			List list = (List) value;
			switch (list.size()) {
			case 0:
				one(null);
				two(null);
				return this;
			case 1:
				one(list.get(0));
				two(null);
				return this;
			}
			one(list.get(0));
			two(list.get(1));
		}
		// 他.
		else {
			one(value);
			two(null);
		}
		return this;
	}

	/**
	 * キーセット.
	 * 
	 * @param one
	 *            １番目のキーを設定します.
	 * @param two
	 *            ２番目のキーを設定します.
	 * @return TwoKey オブジェクトが返却されます.
	 */
	public final TwoKey set(Object one, Object two) {
		if (two == null) {
			return set(one);
		}
		this.one(one);
		this.two(two);
		return this;
	}

	/**
	 * １番目のキーを設定.
	 * 
	 * @param o
	 *            設定情報を設定します.
	 * @return TwoKey オブジェクトが返却されます.
	 */
	public abstract TwoKey one(Object o);

	/**
	 * ２番目のキーを設定.
	 * 
	 * @param o
	 *            設定情報を設定します.
	 * @return TwoKey オブジェクトが返却されます.
	 */
	public abstract TwoKey two(Object o);

	/**
	 * １番目のキーを取得.
	 * 
	 * @return Object １番目のキーが返却されます.
	 */
	public abstract Object one();

	/**
	 * ２番目のキーを取得.
	 * 
	 * @return Object ２番目のキーが返却されます.
	 */
	public abstract Object two();
	
	@Override
	public void out(JniBuffer buf) {
		try {
			toBuffer(buf);
		} catch(Exception e) {
			throw new LeveldbException(e);
		}
	}
	
	@Override
	public int size() {
		return 2;
	}
	
	@Override
	public Object get(int no) {
		switch(no) {
		case 0: return one();
		case 1: return two();
		}
		return null;
	}

}
