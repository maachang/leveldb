package org.maachang.leveldb.util;

/**
 * 取得要素変換.
 */
public interface ConvertGet<N> {

	/**
	 * 取得処理.
	 * 
	 * @parma n 対象の条件を設定します.
	 * @return Object 対象情報が返却されます.
	 */
	Object getOriginal(N n);

	/**
	 * boolean情報を取得.
	 * 
	 * @parma n 対象の条件を設定します.
	 * @return Boolean 情報が返却されます.
	 */
	default Boolean getBoolean(N n) {
		return Utils.convertBool(getOriginal(n));
	}

	/**
	 * int情報を取得.
	 * 
	 * @parma n 対象の条件を設定します.
	 * @return Integer 情報が返却されます.
	 */
	default Integer getInt(N n) {
		return Utils.convertInt(getOriginal(n));
	}

	/**
	 * long情報を取得.
	 * 
	 * @parma n 対象の条件を設定します.
	 * @return Long 情報が返却されます.
	 */
	default Long getLong(N n) {
		return Utils.convertLong(getOriginal(n));
	}

	/**
	 * float情報を取得.
	 * 
	 * @parma n 対象の条件を設定します.
	 * @return Float 情報が返却されます.
	 */
	default Float getFloat(N n) {
		return Utils.convertFloat(getOriginal(n));
	}

	/**
	 * double情報を取得.
	 * 
	 * @parma n 対象の条件を設定します.
	 * @return Double 情報が返却されます.
	 */
	default Double getDouble(N n) {
		return Utils.convertDouble(getOriginal(n));
	}

	/**
	 * String情報を取得.
	 * 
	 * @parma n 対象の条件を設定します.
	 * @return String 情報が返却されます.
	 */
	default String getString(N n) {
		return Utils.convertString(getOriginal(n));
	}

	/**
	 * Date情報を取得.
	 * 
	 * @parma n 対象の条件を設定します.
	 * @return Date 情報が返却されます.
	 */
	default java.sql.Date getDate(N n) {
		return Utils.convertSqlDate(getOriginal(n));
	}

	/**
	 * Time情報を取得.
	 * 
	 * @parma n 対象の条件を設定します.
	 * @return Time 情報が返却されます.
	 */
	default java.sql.Time getTime(N n) {
		return Utils.convertSqlTime(getOriginal(n));
	}

	/**
	 * Timestamp情報を取得.
	 * 
	 * @parma n 対象の条件を設定します.
	 * @return Timestamp 情報が返却されます.
	 */
	default java.sql.Timestamp getTimestamp(N n) {
		return Utils.convertSqlTimestamp(getOriginal(n));
	}
}
