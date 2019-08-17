package org.maachang.leveldb.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.maachang.leveldb.LeveldbException;

/**
 * ユーティリティ系.
 */
public final class Utils {
	protected Utils() {
	};

	/** char文字のチェックを行う配列. **/
	public static final byte[] CHECK_CHAR = new byte[65536];
	static {
		// スペース系は１.
		// ドットは２.
		// 数字の終端文字は３.
		CHECK_CHAR[' '] = 1;
		CHECK_CHAR['\t'] = 1;
		CHECK_CHAR['\r'] = 1;
		CHECK_CHAR['\n'] = 1;
		CHECK_CHAR['.'] = 2;
		CHECK_CHAR['L'] = 3;
		CHECK_CHAR['l'] = 3;
		CHECK_CHAR['F'] = 3;
		CHECK_CHAR['f'] = 3;
		CHECK_CHAR['D'] = 3;
		CHECK_CHAR['d'] = 3;
	}

	/**
	 * 文字列内容が数値かチェック.
	 * 
	 * @param num
	 *            対象のオブジェクトを設定します.
	 * @return boolean [true]の場合、文字列内は数値が格納されています.
	 */
	public static final boolean isNumeric(Object num) {
		if (num == null) {
			return false;
		} else if (num instanceof Number) {
			return true;
		} else if (!(num instanceof String)) {
			num = num.toString();
		}
		char c;
		String s = (String) num;
		int i, start, end, flg, dot;
		start = flg = 0;
		dot = -1;
		end = s.length() - 1;

		for (i = start; i <= end; i++) {
			c = s.charAt(i);
			if (flg == 0 && CHECK_CHAR[c] != 1) {
				if (c == '-') {
					start = i + 1;
				} else {
					start = i;
				}
				flg = 1;
			} else if (flg == 1 && CHECK_CHAR[c] != 0) {
				if (c == '.') {
					if (dot != -1) {
						return false;
					}
					dot = i;
				} else {
					end = i - 1;
					break;
				}
			}
		}
		if (flg == 0) {
			return false;
		}
		if (start <= end) {
			for (i = start; i <= end; i++) {
				if (!((c = s.charAt(i)) == '.' || (c >= '0' && c <= '9'))) {
					return false;
				}
			}
		} else {
			return false;
		}
		return true;
	}

	/**
	 * 対象文字列内容が小数点かチェック.
	 * 
	 * @param n
	 *            対象のオブジェクトを設定します.
	 * @return boolean [true]の場合は、数値内容です.
	 */
	public static final boolean isFloat(Object n) {
		if (Utils.isNumeric(n)) {
			if (n instanceof Float || n instanceof Double
					|| n instanceof BigDecimal) {
				return true;
			} else if (n instanceof String) {
				return ((String) n).indexOf(".") != -1;
			}
			return n.toString().indexOf(".") != -1;
		}
		return false;
	}

	/**
	 * 対象文字列が存在するかチェック.
	 * 
	 * @param v
	 *            対象の情報を設定します.
	 * @return boolean [true]の場合、文字列が存在します.
	 */
	@SuppressWarnings("rawtypes")
	public static final boolean useString(Object v) {
		if (v == null) {
			return false;
		}
		if (v instanceof CharSequence) {
			CharSequence cs = (CharSequence) v;
			if (cs.length() > 0) {
				int len = cs.length();
				for (int i = 0; i < len; i++) {
					if (CHECK_CHAR[cs.charAt(i)] == 1) {
						continue;
					}
					return true;
				}
			}
			return false;
		} else if (v instanceof Collection) {
			return !((Collection) v).isEmpty();
		}
		return true;
	}

	/**
	 * Boolean変換.
	 * 
	 * @param n
	 *            変換対象の条件を設定します.
	 * @return 変換された内容が返却されます.
	 */
	public static final Boolean convertBool(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof Boolean) {
			return (Boolean) o;
		}
		if (o instanceof Number) {
			return (((Number) o).intValue() == 0) ? false : true;
		}
		if (o instanceof String) {
			return Utils.parseBoolean((String) o);
		}
		throw new LeveldbException("BOOL型変換に失敗しました[" + o + "]");
	}

	/**
	 * Integer変換.
	 * 
	 * @param n
	 *            変換対象の条件を設定します.
	 * @return 変換された内容が返却されます.
	 */
	public static final Integer convertInt(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof Integer) {
			return (Integer) o;
		} else if (o instanceof Number) {
			return ((Number) o).intValue();
		} else if (o instanceof String) {
			return Utils.parseInt((String) o);
		}
		throw new LeveldbException("Int型変換に失敗しました[" + o + "]");
	}

	/**
	 * Long変換.
	 * 
	 * @param n
	 *            変換対象の条件を設定します.
	 * @return 変換された内容が返却されます.
	 */
	public static final Long convertLong(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof Long) {
			return (Long) o;
		} else if (o instanceof Number) {
			return ((Number) o).longValue();
		} else if (o instanceof String) {
			return Utils.parseLong((String) o);
		}
		throw new LeveldbException("Long型変換に失敗しました[" + o + "]");
	}

	/**
	 * Float変換.
	 * 
	 * @param n
	 *            変換対象の条件を設定します.
	 * @return 変換された内容が返却されます.
	 */
	public static final Float convertFloat(final Object o) {
		if (o == null) {
			return null;
		} else if (o instanceof Float) {
			return (Float) o;
		} else if (o instanceof Number) {
			return ((Number) o).floatValue();
		} else if (o instanceof String && isNumeric(o)) {
			return parseFloat((String) o);
		}
		throw new LeveldbException("Float型変換に失敗しました[" + o + "]");
	}

	/**
	 * Double変換.
	 * 
	 * @param n
	 *            変換対象の条件を設定します.
	 * @return 変換された内容が返却されます.
	 */
	public static final Double convertDouble(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof Double) {
			return (Double) o;
		} else if (o instanceof Number) {
			return ((Number) o).doubleValue();
		} else if (o instanceof String) {
			return Utils.parseDouble((String) o);
		}
		throw new LeveldbException("Double型変換に失敗しました[" + o + "]");
	}

	/**
	 * 文字列変換.
	 * 
	 * @param n
	 *            変換対象の条件を設定します.
	 * @return 変換された内容が返却されます.
	 */
	public static final String convertString(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof String) {
			return (String) o;
		}
		return o.toString();
	}

	/** 日付のみ表現. **/
	private static final java.sql.Date _cDate(long d) {
		return _cDate(new java.util.Date(d));
	}

	@SuppressWarnings("deprecation")
	private static final java.sql.Date _cDate(java.util.Date n) {
		return new java.sql.Date(n.getYear(), n.getMonth(), n.getDate());
	}

	/**
	 * 日付変換.
	 * 
	 * @param n
	 *            変換対象の条件を設定します.
	 * @return 変換された内容が返却されます.
	 */
	public static final java.sql.Date convertSqlDate(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof java.util.Date) {
			return _cDate(((java.util.Date) o));
		} else if (o instanceof Long) {
			return _cDate((Long) o);
		} else if (o instanceof Number) {
			return _cDate(((Number) o).longValue());
		} else if (o instanceof String) {
			if (isNumeric(o)) {
				return _cDate(parseLong((String) o));
			}
			return DateTimeUtil.getDate((String) o);
		}
		throw new LeveldbException("java.sql.Date型変換に失敗しました[" + o + "]");
	}

	/** 時間のみ表現. **/
	private static final java.sql.Time _cTime(long d) {
		return _cTime(new java.util.Date(d));
	}

	@SuppressWarnings("deprecation")
	private static final java.sql.Time _cTime(java.util.Date n) {
		return new java.sql.Time(n.getHours(), n.getMinutes(), n.getSeconds());
	}

	/**
	 * 時間変換.
	 * 
	 * @param n
	 *            変換対象の条件を設定します.
	 * @return 変換された内容が返却されます.
	 */
	public static final java.sql.Time convertSqlTime(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof java.util.Date) {
			return _cTime((java.util.Date) o);
		} else if (o instanceof Long) {
			return _cTime((Long) o);
		} else if (o instanceof Number) {
			return _cTime(((Number) o).longValue());
		} else if (o instanceof String) {
			if (isNumeric(o)) {
				return _cTime(parseLong((String) o));
			}
			return DateTimeUtil.getTime((String) o);
		}
		throw new LeveldbException("java.sql.Time型変換に失敗しました[" + o + "]");
	}

	/**
	 * 日付時間変換.
	 * 
	 * @param n
	 *            変換対象の条件を設定します.
	 * @return 変換された内容が返却されます.
	 */
	public static final java.sql.Timestamp convertSqlTimestamp(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof java.util.Date) {
			if (o instanceof java.sql.Timestamp) {
				return (java.sql.Timestamp) o;
			}
			return new java.sql.Timestamp(((java.util.Date) o).getTime());
		} else if (o instanceof Long) {
			return new java.sql.Timestamp((Long) o);
		} else if (o instanceof Number) {
			return new java.sql.Timestamp(((Number) o).longValue());
		} else if (o instanceof String) {
			if (isNumeric(o)) {
				return new java.sql.Timestamp(parseLong((String) o));
			}
			return DateTimeUtil.getTimestamp((String) o);
		}
		throw new LeveldbException("java.sql.Timestamp型変換に失敗しました[" + o + "]");
	}

	/**
	 * 通常日付変換.
	 * 
	 * @param n
	 *            変換対象の条件を設定します.
	 * @return 変換された内容が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final java.util.Date convertDate(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof java.util.Date) {
			return (java.util.Date) o;
		} else if (o instanceof Long) {
			return new java.util.Date((Long) o);
		} else if (o instanceof Number) {
			return new java.util.Date(((Number) o).longValue());
		} else if (o instanceof String) {
			if (isNumeric(o)) {
				return new java.sql.Timestamp(parseLong((String) o));
			}
			return DateTimeUtil.getTimestamp((String) o);
		}
		throw new LeveldbException("java.util.Date型変換に失敗しました[" + o + "]");
	}

	/**
	 * 文字列から、Boolean型に変換.
	 * 
	 * @param s
	 *            対象の文字列を設定します.
	 * @return boolean Boolean型が返されます.
	 */
	public static final boolean parseBoolean(String s) {
		char c;
		int i, start, flg, len;

		start = flg = 0;
		len = s.length();

		for (i = start; i < len; i++) {
			c = s.charAt(i);
			if (flg == 0 && CHECK_CHAR[c] != 1) {
				start = i;
				flg = 1;
			} else if (flg == 1 && CHECK_CHAR[c] == 1) {
				len = i;
				break;
			}
		}
		if (flg == 0) {
			throw new LeveldbException("Boolean変換に失敗しました:" + s);
		}

		if (isNumeric(s)) {
			return "0".equals(s) ? false : true;
		} else if (eqEng(s, start, len, "true") || eqEng(s, start, len, "t")
				|| eqEng(s, start, len, "on")) {
			return true;
		} else if (eqEng(s, start, len, "false") || eqEng(s, start, len, "f")
				|| eqEng(s, start, len, "off")) {
			return false;
		}
		throw new LeveldbException("Boolean変換に失敗しました:" + s);
	}

	/**
	 * 文字列から、int型数値に変換.
	 * 
	 * @param num
	 *            対象の文字列を設定します.
	 * @return int int型で変換された数値が返されます.
	 */
	public static final int parseInt(final String num) {
		char c;
		boolean mFlg = false;
		int v, i, len, ret, end;

		ret = end = v = 0;
		len = num.length();

		for (i = end; i < len; i++) {
			c = num.charAt(i);
			if (v == 0 && CHECK_CHAR[c] != 1) {
				if (c == '-') {
					end = i + 1;
					mFlg = true;
				} else {
					end = i;
				}
				v = 1;
			} else if (v == 1 && CHECK_CHAR[c] != 0) {
				len = i;
				break;
			}
		}
		if (v == 0) {
			throw new LeveldbException("Int数値変換に失敗しました:" + num);
		}

		v = 1;
		for (i = len - 1; i >= end; i--) {
			c = num.charAt(i);
			if (c >= '0' && c <= '9') {
				ret += (v * (c - '0'));
				v *= 10;
			} else {
				throw new LeveldbException("Int数値変換に失敗しました:" + num);
			}
		}
		return mFlg ? ret * -1 : ret;
	}

	/**
	 * 文字列から、long型数値に変換.
	 * 
	 * @param num
	 *            対象の文字列を設定します.
	 * @return long long型で変換された数値が返されます.
	 */
	public static final long parseLong(final String num) {
		char c;
		boolean mFlg = false;
		long ret = 0L;
		int len, end, i, flg;

		end = flg = 0;
		len = num.length();

		for (i = end; i < len; i++) {
			c = num.charAt(i);
			if (flg == 0 && CHECK_CHAR[c] != 1) {
				if (c == '-') {
					end = i + 1;
					mFlg = true;
				} else {
					end = i;
				}
				flg = 1;
			} else if (flg == 1 && CHECK_CHAR[c] != 0) {
				len = i;
				break;
			}
		}
		if (flg == 0) {
			throw new LeveldbException("Long数値変換に失敗しました:" + num);
		}

		long v = 1L;
		for (i = len - 1; i >= end; i--) {
			c = num.charAt(i);
			if (c >= '0' && c <= '9') {
				ret += (v * (long) (c - '0'));
				v *= 10L;
			} else {
				throw new LeveldbException("Long数値変換に失敗しました:" + num);
			}
		}
		return mFlg ? ret * -1L : ret;
	}

	/**
	 * 文字列から、float型数値に変換.
	 * 
	 * @param num
	 *            対象の文字列を設定します.
	 * @return float float型で変換された数値が返されます.
	 */
	public static final float parseFloat(final String num) {
		char c;
		boolean mFlg = false;
		float ret = 0f;
		int end, len, flg, dot, i;

		end = flg = 0;
		dot = -1;
		len = num.length();

		for (i = end; i < len; i++) {
			c = num.charAt(i);
			if (flg == 0 && CHECK_CHAR[c] != 1) {
				if (c == '-') {
					end = i + 1;
					mFlg = true;
				} else {
					end = i;
				}
				flg = 1;
			} else if (flg == 1 && CHECK_CHAR[c] != 0) {
				if (c == '.') {
					if (dot != -1) {
						throw new LeveldbException("Float数値変換に失敗しました:" + num);
					}
					dot = i;
				} else {
					len = i;
					break;
				}
			}
		}
		if (flg == 0) {
			throw new LeveldbException("Float数値変換に失敗しました:" + num);
		}

		float v = 1f;
		if (dot == -1) {
			for (i = len - 1; i >= end; i--) {
				c = num.charAt(i);
				if (c >= '0' && c <= '9') {
					ret += (v * (float) (c - '0'));
					v *= 10f;
				} else {
					throw new LeveldbException("Float数値変換に失敗しました:" + num);
				}
			}
			return mFlg ? ret * -1f : ret;
		} else {
			for (i = dot - 1; i >= end; i--) {
				c = num.charAt(i);
				if (c >= '0' && c <= '9') {
					ret += (v * (float) (c - '0'));
					v *= 10f;
				} else {
					throw new LeveldbException("Float数値変換に失敗しました:" + num);
				}
			}
			float dret = 0f;
			v = 1f;
			for (i = len - 1; i > dot; i--) {
				c = num.charAt(i);
				if (c >= '0' && c <= '9') {
					dret += (v * (float) (c - '0'));
					v *= 10f;
				} else {
					throw new LeveldbException("Float数値変換に失敗しました:" + num);
				}
			}
			return mFlg ? (ret + (dret / v)) * -1f : ret + (dret / v);
		}
	}

	/**
	 * 文字列から、double型数値に変換.
	 * 
	 * @param num
	 *            対象の文字列を設定します.
	 * @return double double型で変換された数値が返されます.
	 */
	public static final double parseDouble(final String num) {
		char c;
		boolean mFlg = false;
		double ret = 0d;
		int end, len, flg, dot, i;

		end = flg = 0;
		dot = -1;
		len = num.length();

		for (i = end; i < len; i++) {
			c = num.charAt(i);
			if (flg == 0 && CHECK_CHAR[c] != 1) {
				if (c == '-') {
					end = i + 1;
					mFlg = true;
				} else {
					end = i;
				}
				flg = 1;
			} else if (flg == 1 && CHECK_CHAR[c] != 0) {
				if (c == '.') {
					if (dot != -1) {
						throw new LeveldbException("Double数値変換に失敗しました:" + num);
					}
					dot = i;
				} else {
					len = i;
					break;
				}
			}
		}
		if (flg == 0) {
			throw new LeveldbException("Double数値変換に失敗しました:" + num);
		}

		double v = 1d;
		if (dot == -1) {
			for (i = len - 1; i >= end; i--) {
				c = num.charAt(i);
				if (c >= '0' && c <= '9') {
					ret += (v * (double) (c - '0'));
					v *= 10d;
				} else {
					throw new LeveldbException("Double数値変換に失敗しました:" + num);
				}
			}
			return mFlg ? ret * -1d : ret;
		} else {
			for (i = dot - 1; i >= end; i--) {
				c = num.charAt(i);
				if (c >= '0' && c <= '9') {
					ret += (v * (double) (c - '0'));
					v *= 10d;
				} else {
					throw new LeveldbException("Double数値変換に失敗しました:" + num);
				}
			}
			double dret = 0d;
			v = 1d;
			for (i = len - 1; i > dot; i--) {
				c = num.charAt(i);
				if (c >= '0' && c <= '9') {
					dret += (v * (double) (c - '0'));
					v *= 10d;
				} else {
					throw new LeveldbException("Double数値変換に失敗しました:" + num);
				}
			}
			return mFlg ? (ret + (dret / v)) * -1d : ret + (dret / v);
		}
	}

	/**
	 * boolean群を定義.
	 * 
	 * @param v
	 *            boolean群を設定します.
	 * @return boolean[] boolean群が返却されます.
	 */
	public static final boolean[] getArray(boolean... v) {
		return v;
	}

	/**
	 * int群を定義.
	 * 
	 * @param v
	 *            int群を設定します.
	 * @return int[] int群が返却されます.
	 */
	public static final int[] getArray(int... v) {
		return v;
	}

	/**
	 * long群を定義.
	 * 
	 * @param v
	 *            long群を設定します.
	 * @return long[] long群が返却されます.
	 */
	public static final long[] getArray(long... v) {
		return v;
	}

	/**
	 * float群を定義.
	 * 
	 * @param v
	 *            float群を設定します.
	 * @return float[] float群が返却されます.
	 */
	public static final float[] getArray(float... v) {
		return v;
	}

	/**
	 * double群を定義.
	 * 
	 * @param v
	 *            double群を設定します.
	 * @return double[] double群が返却されます.
	 */
	public static final double[] getArray(double... v) {
		return v;
	}

	/**
	 * java.sql.Date群を定義.
	 * 
	 * @param v
	 *            java.sql.Date群を設定します.
	 * @return java.sql.java.sql.Date[] double群が返却されます.
	 */
	public static final java.sql.Date[] getArray(java.sql.Date... v) {
		return v;
	}

	/**
	 * java.sql.Time群を定義.
	 * 
	 * @param v
	 *            java.sql.Time群を設定します.
	 * @return java.sql.java.sql.Time[] double群が返却されます.
	 */
	public static final java.sql.Time[] getArray(java.sql.Time... v) {
		return v;
	}

	/**
	 * java.sql.Timestamp群を定義.
	 * 
	 * @param v
	 *            java.sql.Timestamp群を設定します.
	 * @return java.sql.java.sql.Timestamp[] double群が返却されます.
	 */
	public static final java.sql.Timestamp[] getArray(java.sql.Timestamp... v) {
		return v;
	}

	/**
	 * String群を定義.
	 * 
	 * @param v
	 *            String群を設定します.
	 * @return String[] String群が返却されます.
	 */
	public static final String[] getArray(String... v) {
		return v;
	}

	/**
	 * Comparable群を定義.
	 * 
	 * @param v
	 *            Comparable群を設定します.
	 * @return Comparable[] Comparable群が返却されます.
	 */
	@SuppressWarnings("rawtypes")
	public static final Comparable[] getArray(Comparable... v) {
		return v;
	}

	/**
	 * Object群を定義.
	 * 
	 * @param v
	 *            Object群を設定します.
	 * @return Object[] Object群が返却されます.
	 */
	public static final Object[] getArray(Object... v) {
		return v;
	}

	/**
	 * ビットマスク長を取得.
	 * 
	 * @param x
	 *            対象の値を設定します.
	 * @return int ビットマスク長が返却されます.
	 */
	public static final int bitMask(int x) {
		if (x < 1) {
			return 1;
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
		x = (x & 0x0000ffff) + (x >> 16 & 0x0000ffff);
		return 1 << (((x & 0x0000ffff) + (x >> 16 & 0x0000ffff)) - 1);
	}

	/**
	 * 英字の大文字小文字を区別しない、バイトチェック.
	 * 
	 * @param s
	 *            比較の文字を設定します.
	 * @param d
	 *            比較の文字を設定します.
	 * @return boolean [true]の場合、一致します.
	 */
	public static final boolean oneEng(char s, char d) {
		return AZ_az.oneEq(s, d);
	}

	/**
	 * 英字の大文字小文字を区別せずにチェック.
	 * 
	 * @param src
	 *            比較元文字を設定します.
	 * @param dest
	 *            比較先文字を設定します.
	 * @return boolean [true]の場合、一致します.
	 */
	public static final boolean eqEng(final String src, final String dest) {
		return AZ_az.eq(src, dest);
	}

	/**
	 * 英字の大文字小文字を区別せずにチェック.
	 * 
	 * @param src
	 *            比較元文字を設定します.
	 * @param start
	 *            srcのチェック開始位置を設定します.
	 * @param end
	 *            srcのチェック終了位置を設定します.
	 * @param dest
	 *            比較先文字を設定します.
	 * @return boolean [true]の場合、一致します.
	 */
	public static final boolean eqEng(String src, int start, int end,
			String dest) {
		return AZ_az.eq(src, start, end - start, dest);
	}

	/**
	 * 英字の大文字小文字を区別しない、文字indexOf.
	 * 
	 * @param buf
	 *            設定対象の文字情報を設定します.
	 * @param chk
	 *            チェック対象の文字情報を設定します.
	 * @param off
	 *            設定対象のオフセット値を設定します.
	 * @return int マッチする位置が返却されます. [-1]の場合は情報は存在しません.
	 */
	public static final int indexOfEng(final String buf, final String chk) {
		return AZ_az.indexOf(buf, chk, 0);
	}

	/**
	 * 英字の大文字小文字を区別しない、文字indexOf.
	 * 
	 * @param buf
	 *            設定対象の文字情報を設定します.
	 * @param chk
	 *            チェック対象の文字情報を設定します.
	 * @param off
	 *            設定対象のオフセット値を設定します.
	 * @return int マッチする位置が返却されます. [-1]の場合は情報は存在しません.
	 */
	public static final int indexOfEng(final String buf, final String chk,
			final int off) {
		return AZ_az.indexOf(buf, chk, off);
	}

	/**
	 * 小文字変換.
	 * 
	 * @param s
	 *            対象の文字列を設定します.
	 * @return String 変換された情報が返却されます.
	 */
	public static final String toLowerCase(String s) {
		return AZ_az.toLowerCase(s);
	}

	/**
	 * 文字情報の置き換え.
	 * 
	 * @param src
	 *            置き換え元の文字列を設定します.
	 * @param s
	 *            置き換え文字条件を設定します.
	 * @param d
	 *            置き換え先の文字条件を設定します.
	 * @return String 文字列が返却されます.
	 */
	public static final String changeString(String src, String s, String d) {
		return changeString(src, 0, src.length(), s, d);
	}

	/**
	 * 文字情報の置き換え.
	 * 
	 * @param src
	 *            置き換え元の文字列を設定します.
	 * @param off
	 *            置き換え元文字のオフセット値を設定します.
	 * @param len
	 *            置き換え元文字の長さを設定します.
	 * @param s
	 *            置き換え文字条件を設定します.
	 * @param d
	 *            置き換え先の文字条件を設定します.
	 * @return String 文字列が返却されます.
	 */
	public static final String changeString(String src, int off, int len,
			String s, String d) {
		int j, k;
		char t = s.charAt(0);
		int lenS = s.length();
		StringBuilder buf = new StringBuilder(len);
		for (int i = off; i < len; i++) {
			if (src.charAt(i) == t) {
				j = i;
				k = 0;
				while (++k < lenS && ++j < len && src.charAt(j) == s.charAt(k))
					;
				if (k >= lenS) {
					buf.append(d);
					i += (lenS - 1);
				} else {
					buf.append(t);
				}
			} else {
				buf.append(src.charAt(i));
			}
		}
		return buf.toString();
	}

	/**
	 * 指定文字内のコーテーションインデントを1つ上げる.
	 * 
	 * @param string
	 *            対象の文字列を設定します.
	 * @param indent
	 *            対象のインデント値を設定します. 0を設定した場合は１つインデントを増やします。
	 *            -1を設定した場合は１つインデントを減らします。
	 * @param dc
	 *            [true]の場合、ダブルコーテーションで処理します.
	 * @return String 変換された文字列が返されます.
	 */
	public static final String indentCote(String string, int indent, boolean dc) {
		if (string == null || string.length() <= 0) {
			return string;
		}
		char cote = (dc) ? '\"' : '\'';
		int len = string.length();
		char c;
		int j;
		int yenLen = 0;
		StringBuilder buf = new StringBuilder((int) (len * 1.25d));
		for (int i = 0; i < len; i++) {
			if ((c = string.charAt(i)) == cote) {
				if (yenLen > 0) {
					if (indent == -1) {
						yenLen >>= 1;
					} else {
						yenLen <<= 1;
					}
					for (j = 0; j < yenLen; j++) {
						buf.append("\\");
					}
					yenLen = 0;
				}
				if (indent == -1) {
					buf.append(cote);
				} else {
					buf.append("\\").append(cote);
				}
			} else if ('\\' == c) {
				yenLen++;
			} else {
				if (yenLen != 0) {
					for (j = 0; j < yenLen; j++) {
						buf.append("\\");
					}
					yenLen = 0;
				}
				buf.append(c);
			}
		}
		if (yenLen != 0) {
			for (j = 0; j < yenLen; j++) {
				buf.append("\\");
			}
		}
		return buf.toString();
	}

	/**
	 * 指定文字内のダブルコーテーションインデントを1つ上げる.
	 * 
	 * @param string
	 *            対象の文字列を設定します.
	 * @return String 変換された文字列が返されます.
	 */
	public static final String upIndentDoubleCote(String string) {
		return indentCote(string, 0, true);
	}

	/**
	 * 指定文字内のシングルコーテーションインデントを1つ上げる.
	 * 
	 * @param string
	 *            対象の文字列を設定します.
	 * @return String 変換された文字列が返されます.
	 */
	public static final String upIndentSingleCote(String string) {
		return indentCote(string, 0, false);
	}

	/**
	 * 指定文字内のダブルコーテーションインデントを1つ下げる.
	 * 
	 * @param string
	 *            対象の文字列を設定します.
	 * @return String 変換された文字列が返されます.
	 */
	public static final String downIndentDoubleCote(String string) {
		// 文字列で検出されるダブルコーテーションが￥始まりの場合は、処理する.
		boolean exec = false;
		int len = string.length();
		char c, b;
		b = 0;
		for (int i = 0; i < len; i++) {
			c = string.charAt(i);
			if (c == '\"') {
				if (b == '\\') {
					exec = true;
				}
				break;
			}
			b = c;
		}
		if (exec) {
			return indentCote(string, -1, true);
		}
		return string;
	}

	/**
	 * 指定文字内のシングルコーテーションインデントを1つ下げる.
	 * 
	 * @param string
	 *            対象の文字列を設定します.
	 * @return String 変換された文字列が返されます.
	 */
	public static final String downIndentSingleCote(String string) {
		// 文字列で検出されるシングルコーテーションが￥始まりの場合は、処理する.
		boolean exec = false;
		int len = string.length();
		char c, b;
		b = 0;
		for (int i = 0; i < len; i++) {
			c = string.charAt(i);
			if (c == '\'') {
				if (b == '\\') {
					exec = true;
				}
				break;
			}
			b = c;
		}
		if (exec) {
			return indentCote(string, -1, false);
		}
		return string;
	}

	/**
	 * バイナリ比較.
	 * 
	 * @param a
	 *            比較元のバイナリを設定します.
	 * @param b
	 *            比較先のバイナリを設定します.
	 * @return int aが大きい場合は[1]、aが小さい場合は[-1]、同じ場合は[0].
	 */
	public static final int binaryCompareTo(byte[] a, byte[] b) {
		int aLen = a.length;
		int bLen = b.length;
		int len = aLen > bLen ? bLen : aLen;
		int ab, bb;
		for (int i = 0; i < len; i++) {
			ab = (int) a[i] & 0xff;
			bb = (int) b[i] & 0xff;
			if (ab > bb) {
				return 1;
			} else if (ab < bb) {
				return -1;
			}
		}
		if (aLen > bLen) {
			return 1;
		} else if (aLen < bLen) {
			return -1;
		}
		return 0;
	}

	/**
	 * バイナリを文字でHex表示.
	 * 
	 * @param b
	 *            対象のバイナリを設定します.
	 * @return String Hex情報が返却されます.
	 */
	public static final String binaryToHexString(byte[] b) {
		String n;
		int len = b.length;
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < len; i++) {
			if (i != 0) {
				buf.append(",");
			}
			n = Integer.toHexString(b[i] & 255);
			buf.append("00".substring(n.length())).append(n);
		}
		return buf.toString();
	}

	/**
	 * 文字列カット. この変換は[cutString]と違い、cutが２文字以上の一致に対応してます.
	 * 
	 * @param str
	 *            対象の文字を設定します.
	 * @param cut
	 *            区切り文字を設定します.
	 * @return List<String> カットされた文字列が返却されます.
	 */
	public static final List<String> cutMString(String str, String cut) {
		int p;
		int b = 0;
		int clen = cut.length();
		List<String> ret = new ArrayList<String>();
		while (true) {
			if ((p = str.indexOf(cut, b)) == -1) {
				ret.add(str.substring(b).trim());
				break;
			}
			ret.add(str.substring(b, p).trim());
			b = p + clen;
		}
		return ret;
	}

	/**
	 * チェック情報単位で情報を区切ります。
	 * 
	 * @param str
	 *            区切り対象の情報を設置します.
	 * @param check
	 *            区切り対象の文字情報をセットします. 区切り対象文字を複数設定する事により、それらに対応した区切りとなります.
	 * @return List 区切られた情報が格納されています.
	 * @exception IllegalArgumentException
	 *                入力例外.
	 */
	public static final List<String> cutString(String str, String check)
			throws IllegalArgumentException {
		ArrayList<String> ret = null;

		ret = new ArrayList<String>();

		cutString(ret, false, str, check);

		return ret;
	}

	/**
	 * チェック情報単位で情報を区切ります。
	 * 
	 * 
	 * @param out
	 *            区切られた情報が格納されます.
	 * @param str
	 *            区切り対象の情報を設置します.
	 * @param check
	 *            区切り対象の文字情報をセットします. 区切り対象文字を複数設定する事により、それらに対応した区切りとなります.
	 * @exception IllegalArgumentException
	 *                入力例外.
	 */
	public static final void cutString(List<String> out, String str,
			String check) throws IllegalArgumentException {
		cutString(out, false, str, check);
	}

	/**
	 * チェック情報単位で情報を区切ります。
	 * 
	 * 
	 * @param mode
	 *            区切られた時の文字列が無い場合に、無視するかチェックします. [true]の場合は、無視しません.
	 *            [false]の場合は、無視します.
	 * @param str
	 *            区切り対象の情報を設置します.
	 * @param check
	 *            区切り対象の文字情報をセットします. 区切り対象文字を複数設定する事により、それらに対応した区切りとなります.
	 * @return List 区切られた情報が格納されています.
	 * @exception IllegalArgumentException
	 *                入力例外.
	 */
	public static final List<String> cutString(boolean mode, String str,
			String check) throws IllegalArgumentException {
		ArrayList<String> ret = null;

		ret = new ArrayList<String>();

		cutString(ret, mode, str, check);

		return ret;
	}

	/**
	 * チェック情報単位で情報を区切ります。
	 * 
	 * 
	 * @param out
	 *            区切られた情報が格納されます.
	 * @param mode
	 *            区切られた時の文字列が無い場合に、無視するかチェックします. [true]の場合は、無視しません.
	 *            [false]の場合は、無視します.
	 * @param str
	 *            区切り対象の情報を設置します.
	 * @param check
	 *            区切り対象の文字情報をセットします. 区切り対象文字を複数設定する事により、それらに対応した区切りとなります.
	 * @exception IllegalArgumentException
	 *                入力例外.
	 */
	public static final void cutString(List<String> out, boolean mode,
			String str, String check) throws IllegalArgumentException {
		int i, j;
		int len;
		int lenJ;
		int s = -1;
		char strCode;

		char[] checkCode = null;
		String tmp = null;

		if (out == null || str == null || (len = str.length()) <= 0
				|| check == null || check.length() <= 0) {
			throw new IllegalArgumentException("引数が不正です");
		}

		out.clear();

		lenJ = check.length();
		checkCode = new char[lenJ];
		check.getChars(0, lenJ, checkCode, 0);

		if (lenJ == 1) {

			for (i = 0, s = -1; i < len; i++) {

				strCode = str.charAt(i);
				s = (s == -1) ? i : s;

				if (strCode == checkCode[0]) {

					if (s < i) {
						tmp = str.substring(s, i);
						out.add(tmp);
						tmp = null;
						s = -1;
					} else if (mode == true) {
						out.add("");
						s = -1;
					} else {
						s = -1;
					}

				}

			}

		} else {

			for (i = 0, s = -1; i < len; i++) {

				strCode = str.charAt(i);
				s = (s == -1) ? i : s;

				for (j = 0; j < lenJ; j++) {
					if (strCode == checkCode[j]) {

						if (s < i) {
							tmp = str.substring(s, i);
							out.add(tmp);
							tmp = null;
							s = -1;
						} else if (mode == true) {
							out.add("");
							s = -1;
						} else {
							s = -1;
						}

						break;

					}
				}

			}

		}

		if (s != -1) {

			tmp = str.substring(s, len);
			out.add(tmp);
			tmp = null;
		}

		checkCode = null;
		tmp = null;
	}

	/**
	 * チェック情報単位で情報を区切ります。
	 * 
	 * 
	 * @param cote
	 *            コーテーション対応であるか設定します. [true]を設定した場合、各コーテーション ( ",' ) で囲った情報内は
	 *            区切り文字と判別しません. [false]を設定した場合、コーテーション対応を行いません.
	 * @param coteFlg
	 *            コーテーションが入っている場合に、コーテーションを範囲に含むか否かを 設定します.
	 *            [true]を設定した場合、コーテーション情報も範囲に含みます.
	 *            [false]を設定した場合、コーテーション情報を範囲としません.
	 * @param str
	 *            区切り対象の情報を設置します.
	 * @param check
	 *            区切り対象の文字情報をセットします. 区切り対象文字を複数設定する事により、それらに対応した区切りとなります.
	 * @return ArrayList 区切られた情報が格納されています.
	 * @exception IllegalArgumentException
	 *                入力例外.
	 */
	public static final List<String> cutString(boolean cote, boolean coteFlg,
			String str, String check) throws IllegalArgumentException {
		ArrayList<String> ret = null;

		ret = new ArrayList<String>();

		cutString(ret, cote, coteFlg, str, check);

		return ret;
	}

	/**
	 * チェック情報単位で情報を区切ります。
	 * 
	 * 
	 * @param out
	 *            区切られた情報が格納されます.
	 * @param cote
	 *            コーテーション対応であるか設定します. [true]を設定した場合、各コーテーション ( ",' ) で囲った情報内は
	 *            区切り文字と判別しません. [false]を設定した場合、コーテーション対応を行いません.
	 * @param coteFlg
	 *            コーテーションが入っている場合に、コーテーションを範囲に含むか否かを 設定します.
	 *            [true]を設定した場合、コーテーション情報も範囲に含みます.
	 *            [false]を設定した場合、コーテーション情報を範囲としません.
	 * @param str
	 *            区切り対象の情報を設置します.
	 * @param check
	 *            区切り対象の文字情報をセットします. 区切り対象文字を複数設定する事により、それらに対応した区切りとなります.
	 * @exception IllegalArgumentException
	 *                入力例外.
	 */
	public static final void cutString(List<String> out, boolean cote,
			boolean coteFlg, String str, String check)
			throws IllegalArgumentException {
		int i, j;
		int len;
		int lenJ;
		int s = -1;

		char coteChr;
		char nowChr;
		char strCode;

		char[] checkCode = null;
		String tmp = null;

		if (cote == false) {
			cutString(out, str, check);
		} else {

			if (out == null || str == null || (len = str.length()) <= 0
					|| check == null || check.length() <= 0) {
				throw new IllegalArgumentException("引数が不正です");
			}

			out.clear();

			lenJ = check.length();
			checkCode = new char[lenJ];
			check.getChars(0, lenJ, checkCode, 0);

			if (lenJ == 1) {
				int befCode = -1;
				boolean yenFlag = false;
				for (i = 0, s = -1, coteChr = 0; i < len; i++) {

					strCode = str.charAt(i);
					nowChr = strCode;
					s = (s == -1) ? i : s;

					if (coteChr == 0) {

						if (nowChr == '\'' || nowChr == '\"') {

							coteChr = nowChr;

							if (s < i) {
								tmp = str.substring(s, i);
								out.add(tmp);
								tmp = null;
								s = -1;
							} else {
								s = -1;
							}

						} else if (strCode == checkCode[0]) {

							if (s < i) {
								tmp = str.substring(s, i);
								out.add(tmp);
								tmp = null;
								s = -1;
							} else {
								s = -1;
							}

						}
					} else {
						if (befCode != '\\' && coteChr == nowChr) {
							yenFlag = false;
							coteChr = 0;

							if (s == i && coteFlg == true) {
								out.add(new StringBuilder().append(strCode)
										.append(strCode).toString());
								s = -1;
							} else if (s < i) {

								if (coteFlg == true) {
									tmp = str.substring(s - 1, i + 1);
								} else {
									tmp = str.substring(s, i);
								}

								out.add(tmp);
								tmp = null;
								s = -1;
							} else {
								s = -1;
							}
						} else if (strCode == '\\' && befCode == '\\') {
							yenFlag = true;
						} else {
							yenFlag = false;
						}
					}
					if (yenFlag) {
						yenFlag = false;
						befCode = -1;
					} else {
						befCode = strCode;
					}
				}
			} else {
				int befCode = -1;
				boolean yenFlag = false;
				for (i = 0, s = -1, coteChr = 0; i < len; i++) {

					strCode = str.charAt(i);
					nowChr = strCode;
					s = (s == -1) ? i : s;

					if (coteChr == 0) {

						if (nowChr == '\'' || nowChr == '\"') {

							coteChr = nowChr;

							if (s < i) {
								tmp = str.substring(s, i);
								out.add(tmp);
								tmp = null;
								s = -1;
							} else {
								s = -1;
							}

						} else {

							for (j = 0; j < lenJ; j++) {

								if (strCode == checkCode[j]) {

									if (s < i) {
										tmp = str.substring(s, i);
										out.add(tmp);
										tmp = null;
										s = -1;
									} else {
										s = -1;
									}

									break;

								}

							}

						}
					} else {
						if (befCode != '\\' && coteChr == nowChr) {

							coteChr = 0;
							yenFlag = false;

							if (s == i && coteFlg == true) {
								out.add(new StringBuilder().append(strCode)
										.append(strCode).toString());
								s = -1;
							} else if (s < i) {

								if (coteFlg == true) {
									tmp = str.substring(s - 1, i + 1);
								} else {
									tmp = str.substring(s, i);
								}

								out.add(tmp);
								tmp = null;
								s = -1;
							} else {
								s = -1;
							}
						} else if (strCode == '\\' && befCode == '\\') {
							yenFlag = true;
						} else {
							yenFlag = false;
						}
					}
					if (yenFlag) {
						yenFlag = false;
						befCode = -1;
					} else {
						befCode = strCode;
					}
				}
			}

			if (s != -1) {

				if (coteChr != 0 && coteFlg == true) {
					tmp = str.substring(s - 1, len) + (char) coteChr;
				} else {
					tmp = str.substring(s, len);
				}

				out.add(tmp);
				tmp = null;

			}

			checkCode = null;
			tmp = null;

		}
	}

	/**
	 * スタックトレースを文字取得.
	 * 
	 * @param t
	 *            対象の例外を設定します.
	 * @return String スタックトレースが返却されます.
	 */
	public static final String getStackTrace(Throwable t) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		t.printStackTrace(pw);
		return sw.toString();
	}
}
