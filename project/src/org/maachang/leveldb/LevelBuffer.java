package org.maachang.leveldb;

/**
 * LeveldbのKey/Value用バッファ管理. このオブジェクトでは、スレッド毎で安全に利用できる JniBufferを管理しています.
 * 普通にJniBufferをnewして利用するのも、問題ありませんが こちらを利用したほうが、再利用の観点から、有利となります.
 */
public final class LevelBuffer {
	protected LevelBuffer() {
	}

	/** キーバッファを、ローカルストレージ単位で保持. **/
	protected static final ThreadLocal<JniBuffer> keyBuffer = new ThreadLocal<JniBuffer>();

	/** 要素バッファをローカルストレージ単位で保持. **/
	protected static final ThreadLocal<JniBuffer> valueBuffer = new ThreadLocal<JniBuffer>();

	/**
	 * キャッシュクリア. ThreadLocalで管理しているキャッシュ情報をクリアします.
	 */
	public static final void clear() {

		// ThreadLocalの内容をクリア.
		JniBuffer v = keyBuffer.get();
		if (v != null) {
			keyBuffer.remove();
			v.destroy();
		}
		v = valueBuffer.get();
		if (v != null) {
			valueBuffer.remove();
			v.destroy();
		}
	}

	/**
	 * KeyBufferを取得.
	 * 
	 * @return JniBuffer KeyBufferが返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final JniBuffer key() throws Exception {
		JniBuffer ret = keyBuffer.get();
		if (ret == null) {
			ret = new JniBuffer();
			keyBuffer.set(ret);
		}
		return ret;
	}

	/**
	 * KeyBufferを取得.
	 * 
	 * @param type
	 *            キータイプを設定します.
	 * @param key
	 *            対象のキー名を設定した場合、その内容がバッファに割り当てられます.
	 * @return JniBuffer KeyBufferが返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final JniBuffer key(int type, Object key) throws Exception {
		return key(type, key, null);
	}

	/**
	 * KeyBufferを取得.
	 * 
	 * @param type
	 *            キータイプを設定します.
	 * @param key
	 *            対象のキー名を設定した場合、その内容がバッファに割り当てられます.
	 * @param twoKey
	 *            ２つ目のキーを設定します.
	 * @return JniBuffer KeyBufferが返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final JniBuffer key(int type, Object key, Object twoKey)
			throws Exception {
		JniBuffer ret = keyBuffer.get();
		if (ret == null) {
			ret = new JniBuffer();
			keyBuffer.set(ret);
		}
		if (key != null) {
			LevelId.buf(type, ret, key, twoKey);
		}
		return ret;
	}

	/**
	 * ValueBufferを取得.
	 * 
	 * @return JniBuffer ValueBufferが返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final JniBuffer value() throws Exception {
		JniBuffer ret = valueBuffer.get();
		if (ret == null) {
			ret = new JniBuffer();
			valueBuffer.set(ret);
		}
		return ret;
	}

	/**
	 * ValueBufferを取得.
	 * 
	 * @param value
	 *            対象の要素を設定した場合、その内容がバッファに割り当てられます.
	 * @return JniBuffer ValueBufferが返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final JniBuffer value(Object value) throws Exception {
		JniBuffer ret = valueBuffer.get();
		if (ret == null) {
			ret = new JniBuffer();
			valueBuffer.set(ret);
		}
		LevelValues.encode(ret, value);
		return ret;
	}

}
