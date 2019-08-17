package org.maachang.leveldb.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * シリアライズ用ユーティリティ.
 */
public final class SerializableUtil {
	private SerializableUtil() {
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
			throw new IllegalArgumentException("引数は不正です");
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

	/**
	 * バイナリをシリアライズオブジェクトに変換.
	 * 
	 * @param bin
	 *            対象のバイナリを設定します.
	 * @return Serializable 変換されたシリアライズオブジェクトが返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final Serializable toObject(byte[] bin) throws Exception {
		if (bin == null || bin.length <= 0) {
			throw new IllegalArgumentException("引数は不正です");
		}
		return toObject(bin, 0, bin.length);
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
	public static final Serializable toObject(byte[] bin, int off, int len)
			throws Exception {
		if (bin == null || bin.length <= 0) {
			throw new IllegalArgumentException("引数は不正です");
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

}
