package org.maachang.leveldb;

import java.lang.reflect.Array;

import org.maachang.leveldb.types.BinBin;
import org.maachang.leveldb.types.BinInt;
import org.maachang.leveldb.types.BinLong;
import org.maachang.leveldb.types.BinStr;
import org.maachang.leveldb.types.IntBin;
import org.maachang.leveldb.types.IntInt;
import org.maachang.leveldb.types.IntLong;
import org.maachang.leveldb.types.IntStr;
import org.maachang.leveldb.types.LongBin;
import org.maachang.leveldb.types.LongInt;
import org.maachang.leveldb.types.LongLong;
import org.maachang.leveldb.types.LongStr;
import org.maachang.leveldb.types.Multi;
import org.maachang.leveldb.types.StrBin;
import org.maachang.leveldb.types.StrInt;
import org.maachang.leveldb.types.StrLong;
import org.maachang.leveldb.types.StrStr;
import org.maachang.leveldb.types.TwoKey;
import org.maachang.leveldb.util.Converter;

/**
 * LevedbID変換処理.
 */
public final class LevelId {
	protected LevelId() {
	}

	/** 変換処理用インターフェイス. **/
	private static interface ConvertCall {

		/**
		 * IDの変換処理.
		 * 
		 * @param value
		 *            対象の変換条件を設定します.
		 * @param value2
		 *            対象の変換条件を設定します.
		 * @return Object 変換された情報が返却されます.
		 */
		public Object id(Object value, Object value2);

		/**
		 * JniBufferに変換処理.
		 * 
		 * @param buf
		 *            JniBufferを設定します.
		 * @param value
		 *            対象の変換条件を設定します.
		 * @param value2
		 *            対象の変換条件を設定します.
		 * @exception Exception
		 *                例外.
		 */
		public void buf(JniBuffer buf, Object value, Object value2) throws Exception;

		/**
		 * JniBufferから取得.
		 * 
		 * @param buf
		 *            JniBufferを設定します.
		 * @return Object 変換された情報が返却されます.
		 * @exception Exception
		 *                例外.
		 */
		public Object get(JniBuffer buf) throws Exception;

		/**
		 * JniBufferから取得.
		 * 
		 * @param out
		 *            キー取得用情報を設定します.
		 * @param buf
		 *            JniBufferを設定します.
		 */
		public void get(Object[] out, JniBuffer buf) throws Exception;
	}

	/** typeに対する変換処理. **/
	private static final ConvertCall[] _CALL = new ConvertCall[] {

		// [0]String.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if (value == null) {
					return "";
				}
				return Converter.convertString(value);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {

				buf.setString(value == null ? "" : Converter.convertString(value));
			}

			public final Object get(JniBuffer buf) throws Exception {
				return buf.getString();
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				out[0] = buf.getString();
			}
		},
		// [1]int
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if (Converter.isNumeric(value)) {
					return Converter.convertInt(value);
				}
				return 0;
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if (Converter.isNumeric(value)) {
					buf.setInt(Converter.convertInt(value));
				} else {
					buf.setInt(0);
				}
			}

			public final Object get(JniBuffer buf) throws Exception {
				return buf.getIntE();
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				out[0] = buf.getIntE();
			}
		},
		// [2]long
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if (Converter.isNumeric(value)) {
					return Converter.convertLong(value);
				}
				return 0L;
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if (Converter.isNumeric(value)) {
					buf.setLong(Converter.convertLong(value));
				} else {
					buf.setLong(0L);
				}
			}

			public final Object get(JniBuffer buf) throws Exception {
				return buf.getLongE();
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				out[0] = buf.getLongE();
			}
		},
		// [3]String-String.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof StrStr) {
					return (StrStr)value;
				}
				return new StrStr(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof StrStr) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = (value != null) ? Converter.convertString(value) : "";
				value2 = (value2 != null) ? Converter.convertString(value2) : "";
				StrStr.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new StrStr(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();
				
				// データ長の位置にセット.
				len -= 4;
				
				// one.
				int oneLen = (int) (JniIO.getIntE(addr, len) & 0x7fffffff);
				if(oneLen == 0) {
					out[0] = "";
				} else {
					out[0] = JniIO.getUtf16(addr, 0, oneLen);
				}

				// two.
				if (len <= oneLen) {
					out[1] = "";
				} else {
					out[1] = JniIO.getUtf16(addr, oneLen, len - oneLen);
				}
			}
		},
		// [4]String-Integer.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof StrInt) {
					return (StrInt)value;
				}
				return new StrInt(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof StrInt) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = (value != null) ? Converter.convertString(value) : "";
				value2 = Converter.isNumeric(value2) ? Converter.convertInt(value2) : 0;
				StrInt.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new StrInt(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();
				
				// データ長の位置にセット.
				len -= 4;

				// one.
				int oneLen = (int) (JniIO.getIntE(addr, len) & 0x7fffffff);
				if(oneLen == 0) {
					out[0] = "";
				} else {
					out[0] = JniIO.getUtf16(addr, 0, oneLen);
				}

				// two.
				out[1] = JniIO.getIntE(addr, oneLen);
			}
		},
		// [5]String-Long.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof StrLong) {
					return (StrLong)value;
				}
				return new StrLong(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof StrLong) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = (value != null) ? Converter.convertString(value) : "";
				value2 = Converter.isNumeric(value2) ? Converter.convertLong(value2) : 0L;
				StrLong.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new StrLong(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();
				
				// データ長の位置にセット.
				len -= 4;

				// one.
				int oneLen = (int) (JniIO.getIntE(addr, len) & 0x7fffffff);
				if(oneLen == 0) {
					out[0] = "";
				} else {
					out[0] = JniIO.getUtf16(addr, 0, oneLen);
				}

				// two.
				out[1] = JniIO.getLongE(addr, oneLen);
			}
		},
		// [6]Integer-String.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof IntStr) {
					return (IntStr)value;
				}
				return new IntStr(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof IntStr) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = Converter.isNumeric(value) ? Converter.convertInt(value) : 0;
				value2 = (value2 != null) ? Converter.convertString(value2) : "";
				IntStr.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new IntStr(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();

				// one.
				out[0] = JniIO.getIntE(addr, 0);

				// two.
				if (len <= 4) {
					out[1] = "";
				} else {
					out[1] = JniIO.getUtf16(addr, 4, len - 4);
				}
			}
		},
		// [7]Integer-Integer.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof IntInt) {
					return (IntInt)value;
				}
				return new IntInt(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof IntInt) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = Converter.isNumeric(value) ? Converter.convertInt(value) : 0;
				value2 = Converter.isNumeric(value2) ? Converter.convertInt(value2) : 0;
				IntInt.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new IntInt(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				long addr = buf.address();

				// one.
				out[0] = JniIO.getIntE(addr, 0);

				// two.
				out[1] = JniIO.getIntE(addr, 4);
			}
		},
		// [8]Integer-Long.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof IntLong) {
					return (IntLong)value;
				}
				return new IntLong(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof IntLong) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = Converter.isNumeric(value) ? Converter.convertInt(value) : 0;
				value2 = Converter.isNumeric(value2) ? Converter.convertLong(value2) : 0L;
				IntLong.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new IntLong(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				long addr = buf.address();

				// one.
				out[0] = JniIO.getIntE(addr, 0);

				// two.
				out[1] = JniIO.getLongE(addr, 4);
			}
		},
		// [9]Long-String.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof LongStr) {
					return (LongStr)value;
				}
				return new LongStr(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof LongStr) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = Converter.isNumeric(value) ? Converter.convertLong(value) : 0L;
				value2 = (value2 != null) ? Converter.convertString(value2) : "";
				LongStr.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new LongStr(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();

				// one.
				out[0] = JniIO.getLongE(addr, 0);

				// two.
				if (len <= 8) {
					out[1] = "";
				} else {
					out[1] = JniIO.getUtf16(addr, 8, len - 8);
				}
			}
		},
		// [10]Long-Integer.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof LongInt) {
					return (LongInt)value;
				}
				return new LongInt(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof LongInt) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = Converter.isNumeric(value) ? Converter.convertLong(value) : 0L;
				value2 = Converter.isNumeric(value2) ? Converter.convertInt(value2) : 0;
				LongInt.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new LongInt(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				long addr = buf.address();

				// one.
				out[0] = JniIO.getLongE(addr, 0);

				// two.
				out[1] = JniIO.getIntE(addr, 8);
			}
		},
		// [11]Long-Long.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof LongLong) {
					return (LongLong)value;
				}
				return new LongLong(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof LongLong) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = Converter.isNumeric(value) ? Converter.convertLong(value) : 0L;
				value2 = Converter.isNumeric(value2) ? Converter.convertLong(value2) : 0L;
				LongLong.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new LongLong(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				long addr = buf.address();

				// one.
				out[0] = JniIO.getLongE(addr, 0);

				// two.
				out[1] = JniIO.getLongE(addr, 8);
			}
		},
		// [12]String-Binary.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof StrBin) {
					return (StrBin)value;
				}
				return new StrBin(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof StrBin) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = value != null ? Converter.convertString(value) : "";
				value2 = value2 == null || !(value2 instanceof byte[]) ? TwoKey.NONE : value2;
				StrBin.convertBuffer(value, (byte[]) value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new StrBin(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();

				// データ長の位置にセット.
				len -= 4;

				// one.
				int oneLen = (int) (JniIO.getIntE(addr, len) & 0x7fffffff);
				if (oneLen == 0) {
					out[0] = "";
				} else {
					out[0] = JniIO.getUtf16(addr, 0, oneLen);
				}

				// two.
				if (len <= oneLen) {
					out[1] = TwoKey.NONE;
				} else {
					len -= oneLen;
					out[1] = new byte[len];
					JniIO.getBinary(addr, oneLen, (byte[]) out[1], 0, len);
				}
			}
		},
		// [13]Int-Binary.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof IntBin) {
					return (IntBin)value;
				}
				return new IntBin(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof IntBin) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = Converter.isNumeric(value) ? Converter.convertInt(value) : 0;
				value2 = value2 == null || !(value2 instanceof byte[]) ? TwoKey.NONE : value2;
				IntBin.convertBuffer(value, (byte[]) value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new IntBin(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();

				// one.
				out[0] = JniIO.getIntE(addr, 0);

				// two.
				len -= 4;
				if (len == 0) {
					out[1] = TwoKey.NONE;
				} else {
					out[1] = new byte[len];
					JniIO.getBinary(addr, 4, (byte[]) out[1], 0, len);
				}
			}
		},
		// [14]Long-Binary.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof LongBin) {
					return (LongBin)value;
				}
				return new LongBin(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof LongBin) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = Converter.isNumeric(value) ? Converter.convertLong(value) : 0L;
				value2 = value2 == null || !(value2 instanceof byte[]) ? TwoKey.NONE : value2;
				LongBin.convertBuffer(value, (byte[]) value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new LongBin(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();

				// one.
				out[0] = JniIO.getLongE(addr, 0);

				// two.
				len -= 8;
				if (len == 0) {
					out[1] = TwoKey.NONE;
				} else {
					out[1] = new byte[len];
					JniIO.getBinary(addr, 8, (byte[]) out[1], 0, len);
				}
			}
		},
		// [15]Binary-String.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof BinStr) {
					return (BinStr)value;
				}
				return new BinStr(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof BinStr) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = value == null || !(value instanceof byte[]) ? TwoKey.NONE : value;
				value2 = (value2 != null) ? Converter.convertString(value2) : "";
				BinStr.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new BinStr(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();

				// データ長の位置にセット.
				len -= 4;

				// one.
				int oneLen = (int) (JniIO.getIntE(addr, len) & 0x7fffffff);
				if (oneLen == 0) {
					out[0] = TwoKey.NONE;
				} else {
					out[0] = new byte[oneLen];
					JniIO.getBinary(addr, 0, (byte[]) out[0], 0, oneLen);
				}

				// two.
				if (len <= oneLen) {
					out[1] = "";
				} else {
					out[1] = JniIO.getUtf16(addr, oneLen, len - oneLen);
				}
			}
		},
		// [16]Binary-Integer.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof BinInt) {
					return (BinInt)value;
				}
				return new BinInt(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof BinInt) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = value == null || !(value instanceof byte[]) ? TwoKey.NONE : value;
				value2 = Converter.isNumeric(value2) ? Converter.convertInt(value2) : 0;
				BinInt.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new BinInt(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();

				// データ長の位置にセット.
				len -= 4;

				// one.
				int oneLen = (int) (JniIO.getIntE(addr, len) & 0x7fffffff);
				if (oneLen == 0) {
					out[0] = TwoKey.NONE;
				} else {
					out[0] = new byte[oneLen];
					JniIO.getBinary(addr, 0, (byte[]) out[0], 0, oneLen);
				}

				// two.
				out[1] = JniIO.getIntE(addr, oneLen);
			}
		},
		// [17]Binary-Long.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof BinLong) {
					return (BinLong)value;
				}
				return new BinLong(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof BinLong) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = value == null || !(value instanceof byte[]) ? TwoKey.NONE : value;
				value2 = Converter.isNumeric(value2) ? Converter.convertLong(value2) : 0L;
				BinLong.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new BinLong(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();
				
				// データ長の位置にセット.
				len -= 4;

				// one.
				int oneLen = (int) (JniIO.getIntE(addr, len) & 0x7fffffff);
				if (oneLen == 0) {
					out[0] = TwoKey.NONE;
				} else {
					out[0] = new byte[oneLen];
					JniIO.getBinary(addr, 0, (byte[]) out[0], 0, oneLen);
				}

				// two.
				out[1] = JniIO.getLongE(addr, oneLen);
			}
		},
		// [18]Binary-Binary.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if(value instanceof BinBin) {
					return (BinBin)value;
				}
				return new BinBin(value, value2);
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if(value instanceof BinBin) {
					((TwoKey)value).toBuffer(buf);
					return;
				}
				value = value == null || !(value instanceof byte[]) ? TwoKey.NONE : value;
				value2 = value2 == null || !(value2 instanceof byte[]) ? TwoKey.NONE : value2;
				BinBin.convertBuffer(value, value2, buf);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new BinBin(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				int len = buf.position();
				long addr = buf.address();
				
				// データ長の位置にセット.
				len -= 4;

				// one.
				int oneLen = (int) (JniIO.getIntE(addr, len) & 0x7fffffff);
				if (oneLen == 0) {
					out[0] = TwoKey.NONE;
				} else {
					out[0] = new byte[oneLen];
					JniIO.getBinary(addr, 0, (byte[]) out[0], 0, oneLen);
				}

				// two.
				if (len <= oneLen) {
					out[1] = TwoKey.NONE;
				} else {
					len -= oneLen;
					out[1] = new byte[len];
					JniIO.getBinary(addr, oneLen, (byte[]) out[1], 0, len);
				}
			}
		},
		// [19]マルチ定義.
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				if (value == null) {
					Multi ret = new Multi();
					ret.add((String) null);
					return ret;
				} else if (value instanceof Multi) {
					return value;
				} else if (value instanceof byte[]) {
					return new Multi((byte[]) value);
				} else if (value.getClass().isArray()) {
					Object o;
					Multi ret = new Multi();
					int len = Array.getLength(value);
					for (int i = 0; i < len; i++) {
						if ((o = Array.get(value, i)) == null) {
							ret.add((String) null);
						} else if (o instanceof String) {
							ret.add((String) o);
						} else if (o instanceof Integer) {
							ret.add((Integer) o);
						} else if (o instanceof Long) {
							ret.add((Long) o);
						} else if (o instanceof Float) {
							ret.add((Float) o);
						} else if (o instanceof Double) {
							ret.add((Double) o);
						} else {
							ret.add((String) null);
						}
					}
					return ret;
				}
				Multi ret = new Multi();
				if (value instanceof String) {
					ret.add((String) value);
				} else if (value instanceof Integer) {
					ret.add((Integer) value);
				} else if (value instanceof Long) {
					ret.add((Long) value);
				} else if (value instanceof Float) {
					ret.add((Float) value);
				} else if (value instanceof Double) {
					ret.add((Double) value);
				} else {
					ret.add((String) null);
				}
				return ret;
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				if (value == null) {
					Multi m = new Multi();
					m.add((String) null);
					m.toBinary(buf);
				} else if (value instanceof Multi) {
					((Multi) value).toBinary(buf);
				} else if (value instanceof byte[]) {
					buf.setBinary((byte[]) value);
				} else if (value.getClass().isArray()) {
					Object o;
					Multi m = new Multi();
					int len = Array.getLength(value);
					for (int i = 0; i < len; i++) {
						if ((o = Array.get(value, i)) == null) {
							m.add((String) null);
						} else if (o instanceof String) {
							m.add((String) o);
						} else if (o instanceof Integer) {
							m.add((Integer) o);
						} else if (o instanceof Long) {
							m.add((Long) o);
						} else if (o instanceof Float) {
							m.add((Float) o);
						} else if (o instanceof Double) {
							m.add((Double) o);
						} else {
							m.add((String) null);
						}
					}
					m.toBinary(buf);
				} else {
					Multi m = new Multi();
					if (value instanceof String) {
						m.add((String) value);
					} else if (value instanceof Integer) {
						m.add((Integer) value);
					} else if (value instanceof Long) {
						m.add((Long) value);
					} else if (value instanceof Float) {
						m.add((Float) value);
					} else if (value instanceof Double) {
						m.add((Double) value);
					} else {
						m.add((String) null);
					}
					m.toBinary(buf);
				}
			}

			public final Object get(JniBuffer buf) throws Exception {
				return new Multi(buf);
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				out[0] = new Multi(buf);
			}
		},
		// [20]自由定義(byte).
		new ConvertCall() {
			public final Object id(Object value, Object value2) {
				return (value instanceof byte[]) ? value : TwoKey.NONE;
			}

			public final void buf(JniBuffer buf, Object value, Object value2) throws Exception {
				buf.setBinary(value == null || !(value instanceof byte[]) ? TwoKey.NONE : (byte[]) value);
			}

			public final Object get(JniBuffer buf) throws Exception {
				return buf.getBinary();
			}

			public final void get(Object[] out, JniBuffer buf) throws Exception {
				out[0] = buf.getBinary();
			}
		}
	};

	/**
	 * 変換処理.
	 * 
	 * @param type
	 *            対象のIDタイプを設定します.
	 * @param value
	 *            変換対象の情報を設定します.
	 * @return Object 変換された情報が返却されます.
	 */
	public static final Object id(int type, Object value) {
		return _CALL[type].id(value, null);
	}

	/**
	 * 変換処理.
	 * 
	 * @param type
	 *            対象のIDタイプを設定します.
	 * @param value
	 *            変換対象の情報を設定します.
	 * @param value2
	 *            対象の変換条件を設定します.
	 * @return Object 変換された情報が返却されます.
	 */
	public static final Object id(int type, Object value, Object value2) {

		// マルチキーの場合.
		if (LevelOption.TYPE_PARAM_LENGTH[type] == 0) {

			// 1キー設定の場合.
			if (value != null && value2 == null) {
				return _CALL[type].id(value, null);
			}
			// 2キー設定の場合.
			else {
				return _CALL[type].id(new Object[] { value, value2 }, null);
			}
		}
		// それ以外.
		else {
			return _CALL[type].id(value, value2);
		}
	}

	/**
	 * 変換処理.
	 * 
	 * @param type
	 *            対象のIDタイプを設定します.
	 * @param value
	 *            変換対象の情報を設定します.
	 * @return Object 変換された情報が返却されます.
	 */
	public static final Object id(int type, Object... value) {
		if (value == null || value.length == 0) {
			return _CALL[type].id(null, null);
		}
		// パラメータ数に応じて、配列をセット.
		switch (LevelOption.TYPE_PARAM_LENGTH[type]) {
		case 1:
			return _CALL[type].id(value[0], null);
		case 2:
			return _CALL[type].id(value[0], value[1]);
		}
		return _CALL[type].id(value, null);
	}

	/**
	 * JniBufferに変換処理.
	 * 
	 * @param type
	 *            対象のIDタイプを設定します.
	 * @param buf
	 *            JniBufferを設定します.
	 * @param value
	 *            対象の変換条件を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final void buf(int type, JniBuffer buf, Object value) throws Exception {
		_CALL[type].buf(buf, value, null);
	}

	/**
	 * JniBufferに変換処理.
	 * 
	 * @param type
	 *            対象のIDタイプを設定します.
	 * @param buf
	 *            JniBufferを設定します.
	 * @param value
	 *            対象の変換条件を設定します.
	 * @param value2
	 *            対象の変換条件を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final void buf(int type, JniBuffer buf, Object value, Object value2) throws Exception {

		// マルチキーの場合.
		if (LevelOption.TYPE_PARAM_LENGTH[type] == 0) {

			// １キー設定の場合.
			if (value != null && value2 == null) {
				_CALL[type].buf(buf, value, null);
			}
			// 2キー設定の場合.
			else {
				_CALL[type].buf(buf, new Object[] { value, value2 }, null);
			}
		}
		// それ以外.
		else {
			_CALL[type].buf(buf, value, value2);
		}
	}

	/**
	 * JniBufferに変換処理.
	 * 
	 * @param type
	 *            対象のIDタイプを設定します.
	 * @param buf
	 *            JniBufferを設定します.
	 * @param value
	 *            対象の変換条件を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final void buf(int type, JniBuffer buf, Object... value) throws Exception {
		if (value == null || value.length == 0) {
			_CALL[type].buf(buf, null, null);
			return;
		}
		// パラメータ数に応じて、配列をセット.
		switch (LevelOption.TYPE_PARAM_LENGTH[type]) {
		case 1:
			_CALL[type].buf(buf, value[0], null);
			return;
		case 2:
			_CALL[type].buf(buf, value[0], value[1]);
			return;
		}
		_CALL[type].buf(buf, value, null);
	}

	/**
	 * JniBufferから取得.
	 * 
	 * @param type
	 *            対象のIDタイプを設定します.
	 * @param buf
	 *            JniBufferを設定します.
	 * @return Object 変換された情報が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final Object get(int type, JniBuffer buf) throws Exception {
		return _CALL[type].get(buf);
	}

	/**
	 * JniBufferから取得.
	 * 
	 * @param out
	 *            取得先の情報を設定します.
	 * @param type
	 *            対象のIDタイプを設定します.
	 * @param buf
	 *            JniBufferを設定します.
	 * @exception Exception
	 *                例外.
	 */
	public static final void get(Object[] out, int type, JniBuffer buf) throws Exception {
		_CALL[type].get(out, buf);
	}
}
