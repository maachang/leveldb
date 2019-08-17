package org.maachang.leveldb;

import org.maachang.leveldb.util.OList;

/**
 * マルチID. バイナリキー系の複数ID管理.
 */
public class MultiId {
	private static final int TYPE_STRING = 0;
	private static final int TYPE_INT = 1;
	private static final int TYPE_LONG = 2;
	private static final int MAX_DATA_LENGTH = 0x00003fff;
	private OList<Object> list;
	private int binaryLength = 0;

	/**
	 * コンストラクタ.
	 */
	public MultiId() {
		list = new OList<Object>();
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param 対象の配列長を設定します
	 *            .
	 */
	public MultiId(int len) {
		list = new OList<Object>(len);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param binary
	 *            対象のバイナリを設定します.
	 */
	public MultiId(byte[] binary) {
		toObject(binary);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param buf
	 *            対象のJniBufferを設定します.
	 */
	public MultiId(JniBuffer buf) {
		toObject(buf);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param args
	 *            パラメータを設定します.
	 */
	public MultiId(Object... args) {
		Object o;
		int len = (args == null || args.length == 0) ? 0 : args.length;
		binaryLength = 0;
		list = new OList<Object>(len);
		for (int i = 0; i < len; i++) {
			if ((o = args[i]) == null) {
				add((String) "");
			} else if (o instanceof String) {
				add((String) o);
			} else if (o instanceof Integer) {
				add((Integer) o);
			} else if (o instanceof Long) {
				add((Long) o);
			} else if (o instanceof Float) {
				add((Float) o);
			} else if (o instanceof Double) {
				add((Double) o);
			} else {
				add((String) "");
			}
		}
	}

	/**
	 * 情報クリア.
	 * 
	 * @param len
	 *            指定以下の値の場合は、この値で配列を作成します.
	 * @return MultiId オブジェクトが返却されます.
	 */
	public MultiId clear(int len) {
		list.clear(len);
		binaryLength = 0;
		return this;
	}

	/**
	 * 情報クリア.
	 * 
	 * @return MultiId オブジェクトが返却されます.
	 */
	public MultiId clear() {
		list.clear();
		binaryLength = 0;
		return this;
	}

	/**
	 * 情報追加.
	 * 
	 * @param args
	 *            対象の情報を設定します.
	 * @return MultiId オブジェクトが返却されます.
	 */
	public MultiId add(Object... args) {
		Object o;
		int len = (args == null || args.length == 0) ? 0 : args.length;
		for (int i = 0; i < len; i++) {
			if ((o = args[i]) == null) {
				add((String) "");
			} else if (o instanceof String) {
				add((String) o);
			} else if (o instanceof Integer) {
				add((Integer) o);
			} else if (o instanceof Long) {
				add((Long) o);
			} else if (o instanceof Float) {
				add((Float) o);
			} else if (o instanceof Double) {
				add((Double) o);
			} else {
				add((String) "");
			}
		}
		return this;
	}

	/**
	 * 情報追加.
	 * 
	 * @param n
	 *            対象の情報を設定します.
	 * @return MultiId オブジェクトが返却されます.
	 */
	public MultiId add(String n) {
		if (n == null) {
			n = "";
		}
		list.add(n);
		binaryLength += (n.length() << 1) + 2;
		return this;
	}

	/**
	 * 情報追加.
	 * 
	 * @param n
	 *            対象の情報を設定します.
	 * @return MultiId オブジェクトが返却されます.
	 */
	public MultiId add(Integer n) {
		if (n == null) {
			n = 0;
		}
		list.add(n);
		binaryLength += 6;
		return this;
	}

	/**
	 * 情報追加.
	 * 
	 * @param n
	 *            対象の情報を設定します.
	 * @return MultiId オブジェクトが返却されます.
	 */
	public MultiId add(Float n) {
		if (n == null) {
			n = 0.0f;
		}
		list.add(n);
		binaryLength += 6;
		return this;
	}

	/**
	 * 情報追加.
	 * 
	 * @param n
	 *            対象の情報を設定します.
	 * @return MultiId オブジェクトが返却されます.
	 */
	public MultiId add(Long n) {
		if (n == null) {
			n = 0L;
		}
		list.add(n);
		binaryLength += 10;
		return this;
	}

	/**
	 * 情報追加.
	 * 
	 * @param n
	 *            対象の情報を設定します.
	 * @return MultiId オブジェクトが返却されます.
	 */
	public MultiId add(Double n) {
		if (n == null) {
			n = 0.0d;
		}
		list.add(n);
		binaryLength += 10;
		return this;
	}

	/**
	 * 情報取得.
	 * 
	 * @param no
	 *            対象の項番を設定します.
	 * @return Object 情報が返却されます.
	 */
	public Object get(int no) {
		return list.get(no);
	}

	/**
	 * 登録情報数を取得.
	 * 
	 * @return int 登録情報数が返却されます.
	 */
	public int size() {
		return list.size();
	}

	/**
	 * バイナリ変換.
	 * 
	 * @return byte[] バイナリが返却されます.
	 */
	public byte[] toBinary() {
		int len = list.size();
		if (len == 0) {
			return null;
		}
		Object o;
		String s;
		int oLen;
		byte[] ret = new byte[binaryLength + 1];
		ret[binaryLength] = (byte) (len & 255);
		int off = 0;
		int endOff = binaryLength;
		for (int i = 0; i < len; i++) {
			o = list.get(i);

			// 数値系の場合.
			if (o instanceof Number) {

				// 32bit整数セット.
				if (o instanceof Integer) {
					putInteger(ret, off, (Integer) o);
					off += 4;
					putEndLength(ret, endOff - 2, TYPE_INT, 0);
					endOff -= 2;
				}
				// 64bit整数セット.
				else if (o instanceof Long) {
					putLong(ret, off, (Long) o);
					off += 8;
					putEndLength(ret, endOff - 2, TYPE_LONG, 0);
					endOff -= 2;
				}
				// 32bit浮動小数点をセット.
				else if (o instanceof Float) {
					putInteger(ret, off, Float.floatToIntBits((Float) o));
					off += 4;
					putEndLength(ret, endOff - 2, TYPE_INT, 1);
					endOff -= 2;
				}
				// 64bit整数セット.
				// 64bit浮動小数点セット.
				else {
					putLong(ret, off, Double.doubleToLongBits((Double) o));
					off += 8;
					putEndLength(ret, endOff - 2, TYPE_LONG, 1);
					endOff -= 2;
				}
			}
			// 文字列セット.
			else if (o instanceof String) {
				s = (String) o;
				oLen = s.length();
				if (oLen > MAX_DATA_LENGTH) {
					oLen = MAX_DATA_LENGTH;
				}
				putString(ret, off, s, oLen);
				off += oLen << 1;
				putEndLength(ret, endOff - 2, TYPE_STRING, oLen << 1);
				endOff -= 2;
			}
		}
		return ret;
	}

	/**
	 * バイナリ変換.
	 * 
	 * @param out
	 *            出力先のJniBufferを設定します.
	 */
	public void toBinary(JniBuffer out) {
		int len = list.size();
		if (len == 0) {
			out.clear();
			return;
		}
		// 今回データ長をセット.
		out.position(binaryLength + 1);
		long addr = out.address;

		// データ数をセット.
		JniIO.put(addr, binaryLength, (byte) (len & 255));

		Object o;
		String s;
		int oLen;
		int off = 0;
		int endOff = binaryLength;
		for (int i = 0; i < len; i++) {
			o = list.get(i);

			// 数値系の場合.
			if (o instanceof Number) {

				// 32bit整数セット.
				if (o instanceof Integer) {
					putInteger(addr, off, (Integer) o);
					off += 4;
					putEndLength(addr, endOff - 2, TYPE_INT, 0);
					endOff -= 2;
				}
				// 64bit整数セット.
				else if (o instanceof Long) {
					putLong(addr, off, (Long) o);
					off += 8;
					putEndLength(addr, endOff - 2, TYPE_LONG, 0);
					endOff -= 2;
				}
				// 32bit浮動小数点をセット.
				else if (o instanceof Float) {
					putInteger(addr, off, Float.floatToIntBits((Float) o));
					off += 4;
					putEndLength(addr, endOff - 2, TYPE_INT, 1);
					endOff -= 2;
				}
				// 64bit整数セット.
				// 64bit浮動小数点セット.
				else {
					putLong(addr, off, Double.doubleToLongBits((Double) o));
					off += 8;
					putEndLength(addr, endOff - 2, TYPE_LONG, 1);
					endOff -= 2;
				}
			}
			// 文字列セット.
			else if (o instanceof String) {
				s = (String) o;
				oLen = s.length();
				if (oLen > MAX_DATA_LENGTH) {
					oLen = MAX_DATA_LENGTH;
				}
				putString(addr, off, s, oLen);
				off += oLen << 1;
				putEndLength(addr, endOff - 2, TYPE_STRING, oLen << 1);
				endOff -= 2;
			}
		}
	}

	/**
	 * バイナリ復元.
	 * 
	 * @param binary
	 *            対象のバイナリを設定します.
	 * @return MultiId オブジェクトが返却されます.
	 */
	public MultiId toObject(byte[] binary) {
		if (binary == null || binary.length == 0) {
			if (list == null) {
				list = new OList<Object>();
				binaryLength = 0;
			} else {
				clear();
			}
			return this;
		}
		int len = binary[binary.length - 1] & 255;
		if (list == null) {
			list = new OList<Object>(len);
		} else {
			list.clear(len);
		}
		binaryLength = binary.length - 1;

		char[] c;
		int j, oneLen, off, endOff, n;
		long l;
		off = 0;
		endOff = binaryLength - 1;
		for (int i = 0; i < len; i++) {

			// 長さを取得.
			oneLen = (((binary[endOff - 1]) & 0x3f) << 8)
					| (binary[endOff] & 255);

			// タイプを取得.
			switch (((binary[endOff - 1] & 0xc0) >> 6) & 0x03) {

			// 文字列.
			case TYPE_STRING:
				if (oneLen == 0) {
					list.add("");
				} else {
					oneLen >>= 1;
					c = new char[oneLen];
					for (j = 0; j < oneLen; j++) {
						c[j] = (char) (((binary[off] << 8) & 0xff00) | (binary[off + 1] & 0x00ff));
						off += 2;
					}
					list.add(new String(c, 0, oneLen));
					c = null;
				}
				break;

			// 数字(32bit).
			case TYPE_INT:
				n = (int) (((binary[off] & 255) << 24)
						| ((binary[off + 1] & 255) << 16)
						| ((binary[off + 2] & 255) << 8) | (binary[off + 3] & 255)) ^ 0x80000000;
				if (oneLen == 0) {
					list.add(n);
				} else {
					list.add(Float.intBitsToFloat(n));
				}
				off += 4;
				break;

			// 数字(64bit).
			case TYPE_LONG:
				l = (long) (((binary[off] & 255L) << 56L)
						| ((binary[off + 1] & 255L) << 48L)
						| ((binary[off + 2] & 255L) << 40L)
						| ((binary[off + 3] & 255L) << 32L)
						| ((binary[off + 4] & 255L) << 24L)
						| ((binary[off + 5] & 255L) << 16L)
						| ((binary[off + 6] & 255L) << 8L) | (binary[off + 7] & 255L)) ^ 0x8000000000000000L;
				if (oneLen == 0) {
					list.add(l);
				} else {
					list.add(Double.longBitsToDouble(l));
				}
				off += 8;
				break;
			default:
				throw new LeveldbException("バイナリ復元に失敗しました");
			}
			endOff -= 2;
		}
		return this;
	}

	/**
	 * バイナリ復元.
	 * 
	 * @param buf
	 *            対象のJniBufferオブジェクトを設定します.
	 * @return MultiId オブジェクトが返却されます.
	 */
	public MultiId toObject(JniBuffer buf) {
		if (buf.position == 0) {
			if (list == null) {
				list = new OList<Object>();
				binaryLength = 0;
			} else {
				clear();
			}
			return this;
		}
		long addr = buf.address;
		int len = JniIO.get(addr, buf.position - 1) & 255;
		if (list == null) {
			list = new OList<Object>(len);
		} else {
			list.clear(len);
		}
		binaryLength = buf.position - 1;

		char[] c;
		int j, oneLen, off, endOff, n;
		long l;
		off = 0;
		endOff = binaryLength - 1;
		for (int i = 0; i < len; i++) {

			// 長さを取得.
			oneLen = (((JniIO.get(addr, endOff - 1)) & 0x3f) << 8)
					| (JniIO.get(addr, endOff) & 255);

			// タイプを取得.
			switch (((JniIO.get(addr, endOff - 1) & 0xc0) >> 6) & 0x03) {

			// 文字列.
			case TYPE_STRING:
				if (oneLen == 0) {
					list.add("");
				} else {
					oneLen >>= 1;
					c = new char[oneLen];
					for (j = 0; j < oneLen; j++) {
						c[j] = (char) (((JniIO.get(addr, off) << 8) & 0xff00) | (JniIO
								.get(addr, off + 1) & 0x00ff));
						off += 2;
					}
					list.add(new String(c, 0, oneLen));
					c = null;
				}
				break;

			// 数字(32bit).
			case TYPE_INT:
				n = (int) (((JniIO.get(addr, off) & 255) << 24)
						| ((JniIO.get(addr, off + 1) & 255) << 16)
						| ((JniIO.get(addr, off + 2) & 255) << 8) | (JniIO.get(
						addr, off + 3) & 255)) ^ 0x80000000;
				if (oneLen == 0) {
					list.add(n);
				} else {
					list.add(Float.intBitsToFloat(n));
				}
				off += 4;
				break;

			// 数字(64bit).
			case TYPE_LONG:
				l = (long) (((JniIO.get(addr, off) & 255L) << 56L)
						| ((JniIO.get(addr, off + 1) & 255L) << 48L)
						| ((JniIO.get(addr, off + 2) & 255L) << 40L)
						| ((JniIO.get(addr, off + 3) & 255L) << 32L)
						| ((JniIO.get(addr, off + 4) & 255L) << 24L)
						| ((JniIO.get(addr, off + 5) & 255L) << 16L)
						| ((JniIO.get(addr, off + 6) & 255L) << 8L) | (JniIO
						.get(addr, off + 7) & 255L)) ^ 0x8000000000000000L;
				if (oneLen == 0) {
					list.add(l);
				} else {
					list.add(Double.longBitsToDouble(l));
				}
				off += 8;
				break;
			default:
				throw new LeveldbException("バイナリ復元に失敗しました");
			}
			endOff -= 2;
		}
		return this;
	}

	/** １つのデータ長を設定. **/
	private static final void putEndLength(byte[] b, int pos, int type, int len) {
		b[pos] = (byte) ((type << 6) | ((((len & 0x0000ff00) >> 8)) & 0x3f));
		b[pos + 1] = (byte) (len & 255);
	}

	/** 文字列をセット. **/
	private static final void putString(byte[] b, int pos, String s, int len) {
		char c;
		for (int i = 0; i < len; i++) {
			b[pos] = (byte) (((c = s.charAt(i)) & 0xff00) >> 8);
			b[pos + 1] = (byte) c;
			pos += 2;
		}
	}

	/** 32bit整数をセット. **/
	private static final void putInteger(byte[] b, int pos, int n) {
		// マイナスフラグを反転.
		n = n ^ 0x80000000;
		b[pos] = (byte) ((n >> 24) & 255);
		b[pos + 1] = (byte) ((n >> 16) & 255);
		b[pos + 2] = (byte) ((n >> 8) & 255);
		b[pos + 3] = (byte) (n & 255);
	}

	/** 32bit整数をセット. **/
	private static final void putLong(byte[] b, int pos, long n) {
		// マイナスフラグを反転.
		n = n ^ 0x8000000000000000L;
		b[pos] = (byte) ((n >> 56L) & 255L);
		b[pos + 1] = (byte) ((n >> 48L) & 255L);
		b[pos + 2] = (byte) ((n >> 40L) & 255L);
		b[pos + 3] = (byte) ((n >> 32L) & 255L);
		b[pos + 4] = (byte) ((n >> 24L) & 255L);
		b[pos + 5] = (byte) ((n >> 16L) & 255L);
		b[pos + 6] = (byte) ((n >> 8L) & 255L);
		b[pos + 7] = (byte) (n & 255L);
	}

	/** １つのデータ長を設定(JniBuffer). **/
	private static final void putEndLength(long addr, int pos, int type, int len) {
		JniIO.put(addr, pos,
				(byte) ((type << 6) | ((((len & 0x0000ff00) >> 8)) & 0x3f)));
		JniIO.put(addr, pos + 1, (byte) (len & 255));
	}

	/** 文字列をセット(JniBuffer). **/
	private static final void putString(long addr, int pos, String s, int len) {
		char c;
		for (int i = 0; i < len; i++) {
			JniIO.put(addr, pos, (byte) (((c = s.charAt(i)) & 0xff00) >> 8));
			JniIO.put(addr, pos + 1, (byte) c);
			pos += 2;
		}
	}

	/** 32bit整数をセット(JniBuffer). **/
	private static final void putInteger(long addr, int pos, int n) {
		// マイナスフラグを反転.
		n = n ^ 0x80000000;
		JniIO.put(addr, pos, (byte) ((n >> 24) & 255));
		JniIO.put(addr, pos + 1, (byte) ((n >> 16) & 255));
		JniIO.put(addr, pos + 2, (byte) ((n >> 8) & 255));
		JniIO.put(addr, pos + 3, (byte) (n & 255));
	}

	/** 32bit整数をセット(JniBuffer). **/
	private static final void putLong(long addr, int pos, long n) {
		// マイナスフラグを反転.
		n = n ^ 0x8000000000000000L;
		JniIO.put(addr, pos, (byte) ((n >> 56L) & 255L));
		JniIO.put(addr, pos + 1, (byte) ((n >> 48L) & 255L));
		JniIO.put(addr, pos + 2, (byte) ((n >> 40L) & 255L));
		JniIO.put(addr, pos + 3, (byte) ((n >> 32L) & 255L));
		JniIO.put(addr, pos + 4, (byte) ((n >> 24L) & 255L));
		JniIO.put(addr, pos + 5, (byte) ((n >> 16L) & 255L));
		JniIO.put(addr, pos + 6, (byte) ((n >> 8L) & 255L));
		JniIO.put(addr, pos + 7, (byte) (n & 255L));
	}

}
