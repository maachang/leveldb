package org.maachang.leveldb.util;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Json変換処理.
 */
@SuppressWarnings({"rawtypes"})
public final class Json {
	private Json() {
	}

	private static final int TYPE_ARRAY = 0;
	private static final int TYPE_MAP = 1;

	private static final String DECODE_BASE_64 = "atob";
	private static final int DECODE_BASE_64_LEN = DECODE_BASE_64.length();

	private static final String NEW_DATE = "Date";
	private static final int NEW_DATE_LEN = NEW_DATE.length();

	private static final String JAVA_SERIALIZABLE = "serial";
	private static final int JAVA_SERIALIZABLE_LEN = JAVA_SERIALIZABLE.length();

	/**
	 * JSON変換.
	 * 
	 * @param target
	 *            対象のターゲットオブジェクトを設定します.
	 * @return String 変換されたJSON情報が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final String encodeJSON(Object target) throws Exception {
		return encodeJSON(false, target);
	}

	/**
	 * JSON変換.
	 * 
	 * @param mode
	 *            [true]の場合、byte配列条件に対して、Base64変換処理を行います.
	 * @param target
	 *            対象のターゲットオブジェクトを設定します.
	 * @return String 変換されたJSON情報が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final String encodeJSON(boolean mode, Object target)
			throws Exception {
		StringBuilder buf = new StringBuilder();
		_encode(mode, buf, target, target);
		return buf.toString();
	}

	/**
	 * JSON形式から、オブジェクト変換2.
	 * <p>
	 * 自前変換処理
	 * </p>
	 * <p>
	 * 処理は、通常よりも１００倍程度速いが、機能が限定的
	 * </p>
	 * 
	 * @param context
	 *            対象のコンテキストを設定します.
	 * @param json
	 *            対象のJSON情報を設定します.
	 * @return Object 変換されたJSON情報が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final Object decodeJSON(String json) throws Exception {
		return decodeJSON(false, json);
	}

	/**
	 * JSON形式から、オブジェクト変換2.
	 * <p>
	 * 自前変換処理
	 * </p>
	 * <p>
	 * 処理は、通常よりも１００倍程度速いが、機能が限定的
	 * </p>
	 * 
	 * @param mode
	 *            [true]を設定した場合、MapオブジェクトをConcurrentHashMapで生成します.
	 * @param context
	 *            対象のコンテキストを設定します.
	 * @param json
	 *            対象のJSON情報を設定します.
	 * @return Object 変換されたJSON情報が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final Object decodeJSON(boolean mode, String json)
			throws Exception {
		//
		// Javaで解析.
		// 速度は速いが、一定条件のみ.
		// 対応条件: byte変換:decodeBase64
		// Date対応:new Date()
		// それ以外は非対応.
		if (json == null) {
			return null;
		}
		// コメントを除外.
		if ((json = ParseUtil.cutComment(json.trim()).trim()).length() == 0) {
			return json;
		}

		List<Object> list;
		int[] n = new int[1];
		while (true) {
			// token解析が必要な場合.
			if (json.startsWith("[") || json.startsWith("{")) {
				// JSON形式をToken化.
				list = analysisJsonToken(json);
				// Token解析処理.
				if ("[".equals(list.get(0))) {
					// List解析.
					return createJsonInfo(mode, n, list, TYPE_ARRAY, 0, list
							.size());
				} else {
					// Map解析.
					return createJsonInfo(mode, n, list, TYPE_MAP, 0, list
							.size());
				}
			} else if (json.startsWith("(") && json.endsWith(")")) {
				json = json.substring(1, json.length() - 1).trim();
				continue;
			}
			break;
		}
		return decJsonValue(n, 0, json);
	}

	/** [encodeJSON]jsonコンバート. **/
	private static final void _encode(boolean mode, StringBuilder buf,
			Object base, Object target) throws Exception {
		if (target instanceof Map) {
			encodeJsonMap(mode, buf, base, (Map) target);
		} else if (target instanceof List) {
			encodeJsonList(mode, buf, base, (List) target);
		} else if (target instanceof Long) {
			// longの場合、javascriptでは、倍角浮動小数点のため、
			// 大きな64ビット整数を扱うと、丸められるため、
			// 一定以上の大きさの場合(１５桁(マイナスの場合は１６桁）を超える場合)
			// は、文字列で渡す.
			long n = (Long) target;
			if (n >= 0L) {
				if ((n & 0x7ff0000000000000L) != 0L) {
					buf.append("\"").append(target).append("\"");
				} else {
					buf.append(target);
				}
			} else if (((~n) & 0x7ff0000000000000L) != 0) {
				buf.append("\"").append(target).append("\"");
			} else {
				buf.append(target);
			}
		} else if (target instanceof Short || target instanceof Integer
				|| target instanceof Float || target instanceof Double
				|| target instanceof BigInteger || target instanceof BigDecimal) {
			buf.append(target);
		} else if (target instanceof Character || target instanceof String) {
			buf.append("\"").append(target).append("\"");
		} else if (target instanceof byte[]) {
			if (mode) {
				buf.append(DECODE_BASE_64).append("(").append("\"").append(
						Base64.encode((byte[]) target)).append("\"")
						.append(")");
			} else {
				buf.append("null");
			}
		} else if (target instanceof char[]) {
			buf.append("\"").append(
					new String((char[]) target, 0, ((char[]) target).length))
					.append("\"");
		} else if (target instanceof java.util.Date) {
			buf.append("new Date(").append(((java.util.Date) target).getTime())
					.append(")");
		} else if (target instanceof Boolean) {
			buf.append(target);
		} else if (target == null) {
			buf.append("null");
		} else if (target.getClass().isArray()) {
			if (Array.getLength(target) == 0) {
				buf.append("[]");
			} else {
				encodeJsonArray(mode, buf, base, target);
			}
		}
		// シリアライズオブジェクトの変換.
		else if (target instanceof Serializable) {
			if (mode) {
				buf.append(JAVA_SERIALIZABLE).append("(").append("\"").append(
						Base64.encode(SerializableUtil
								.toBinary((Serializable) target))).append("\"")
						.append(")");
			} else {
				buf.append("null");
			}
		} else {
			buf.append("null");
		}
	}

	/** [encodeJSON]jsonMapコンバート. **/
	private static final void encodeJsonMap(boolean mode, StringBuilder buf,
			Object base, Map map) throws Exception {
		boolean flg = false;
		Map mp = (Map) map;
		Iterator it = mp.keySet().iterator();
		buf.append("{");
		while (it.hasNext()) {
			String key = (String) it.next();
			Object value = mp.get(key);
			if (base == value) {
				continue;
			}
			if (flg) {
				buf.append(",");
			}
			flg = true;
			buf.append("\"").append(key).append("\":");
			_encode(mode, buf, base, value);
		}
		buf.append("}");
	}

	/** [encodeJSON]jsonListコンバート. **/
	private static final void encodeJsonList(boolean mode, StringBuilder buf,
			Object base, List list) throws Exception {
		boolean flg = false;
		List lst = (List) list;
		buf.append("[");
		int len = lst.size();
		for (int i = 0; i < len; i++) {
			Object value = lst.get(i);
			if (base == value) {
				continue;
			}
			if (flg) {
				buf.append(",");
			}
			flg = true;
			_encode(mode, buf, base, value);
		}
		buf.append("]");
	}

	/** [encodeJSON]json配列コンバート. **/
	private static final void encodeJsonArray(boolean mode, StringBuilder buf,
			Object base, Object list) throws Exception {
		boolean flg = false;
		int len = Array.getLength(list);
		buf.append("[");
		for (int i = 0; i < len; i++) {
			Object value = Array.get(list, i);
			if (base == value) {
				continue;
			}
			if (flg) {
				buf.append(",");
			}
			flg = true;
			_encode(mode, buf, base, value);
		}
		buf.append("]");
	}

	/** [decodeJSON]１つの要素を変換. **/
	private static final Object decJsonValue(int[] n, int no, String json)
			throws Exception {
		// json = json.trim() ;
		int len;
		if ((len = json.length()) <= 0) {
			return json;
		}
		// 文字列コーテーション区切り.
		if ((json.startsWith("\"") && json.endsWith("\""))
				|| (json.startsWith("\'") && json.endsWith("\'"))) {
			return json.substring(1, len - 1);
		}
		// NULL文字.
		else if ("null".equals(json)) {
			return null;
		}
		// BOOLEAN true.
		else if ("true".equals(json)) {
			return Boolean.TRUE;
		}
		// BOOLEAN false.
		else if ("false".equals(json)) {
			return Boolean.FALSE;
		}
		// 数値.
		if (Utils.isNumeric(json)) {
			if (json.indexOf(".") != -1) {
				return Utils.parseDouble(json);
			}
			return Utils.parseLong(json);
		}
		// new Date.
		else if (json.startsWith("new")) {
			int x = ParseUtil.indexByEquals(json, NEW_DATE, 0);
			if (x != -1) {
				int b = x + NEW_DATE_LEN;
				x = ParseUtil.indexParAndCote(json, '(', ')', b);
				b = ParseUtil.indexByEquals(json, "(", b);
				if (x == -1) {
					throw new IOException("JSON解析に失敗(" + json + "):No:" + no);
				}
				String s = json.substring(b + 1, x).trim();
				if (s.length() <= 0) {
					return new Date();
				}
				if ((s.startsWith("'") && s.endsWith("'"))
						|| (s.startsWith("\"") && s.endsWith("\""))) {
					s = s.substring(1, s.length() - 1).trim();
				}
				return new Date(Utils.parseLong(s));
			}
		}
		// decodeBase64.
		else if (json.startsWith(DECODE_BASE_64)) {
			int b = DECODE_BASE_64_LEN;
			int x = ParseUtil.indexParAndCote(json, '(', ')', b);
			b = ParseUtil.indexByEquals(json, "(", b);
			if (x == -1) {
				throw new IOException("JSON解析に失敗(" + json + "):No:" + no);
			}
			String s = json.substring(b + 1, x).trim();
			if ((s.startsWith("'") && s.endsWith("'"))
					|| (s.startsWith("\"") && s.endsWith("\""))) {
				s = s.substring(1, s.length() - 1).trim();
			}
			return Base64.decode(s);
		}
		// serializable.
		else if (json.startsWith(JAVA_SERIALIZABLE)) {
			int b = JAVA_SERIALIZABLE_LEN;
			int x = ParseUtil.indexParAndCote(json, '(', ')', b);
			b = ParseUtil.indexByEquals(json, "(", b);
			if (x == -1) {
				throw new IOException("JSON解析に失敗(" + json + "):No:" + no);
			}
			String s = json.substring(b + 1, x).trim();
			if ((s.startsWith("'") && s.endsWith("'"))
					|| (s.startsWith("\"") && s.endsWith("\""))) {
				s = s.substring(1, s.length() - 1).trim();
			}
			return SerializableUtil.toObject(Base64.decode(s));
		}
		// その他.
		throw new IOException("JSON解析に失敗(" + json + "):No:" + no);
	}

	/** JSON_Token_解析処理 **/
	private static final List<Object> analysisJsonToken(String json)
			throws Exception {
		int s = -1;
		char c;
		int cote = -1;
		int bef = -1;
		int len = json.length();
		List<Object> ret = new ArrayList<Object>();
		// Token解析.
		for (int i = 0; i < len; i++) {
			c = json.charAt(i);
			// コーテーション内.
			if (cote != -1) {
				// コーテーションの終端.
				if (bef != '\\' && cote == c) {
					ret.add(json.substring(s - 1, i + 1));
					cote = -1;
					s = i + 1;
				}
			}
			// コーテーション開始.
			else if (bef != '\\' && (c == '\'' || c == '\"')) {
				cote = c;
				if (s != -1 && s != i && bef != ' ' && bef != '　'
						&& bef != '\t' && bef != '\n' && bef != '\r') {
					ret.add(json.substring(s, i + 1));
				}
				s = i + 1;
				bef = -1;
			}
			// ワード区切り.
			else if (c == '[' || c == ']' || c == '{' || c == '}' || c == '('
					|| c == ')' || c == ':' || c == ';' || c == ','
					|| (c == '.' && (bef < '0' || bef > '9'))) {
				if (s != -1 && s != i && bef != ' ' && bef != '　'
						&& bef != '\t' && bef != '\n' && bef != '\r') {
					ret.add(json.substring(s, i));
				}
				ret.add(new String(new char[] { c }, 0, 1));
				s = i + 1;
			}
			// 連続空間区切り.
			else if (c == ' ' || c == '　' || c == '\t' || c == '\n'
					|| c == '\r') {
				if (s != -1 && s != i && bef != ' ' && bef != '　'
						&& bef != '\t' && bef != '\n' && bef != '\r') {
					ret.add(json.substring(s, i));
				}
				s = -1;
			}
			// その他文字列.
			else if (s == -1) {
				s = i;
			}
			bef = c;
		}
		return ret;
	}

	/** Json-Token解析. **/
	private static final Object createJsonInfo(boolean mode, int[] n,
			List<Object> token, int type, int no, int len) throws Exception {
		String value;
		StringBuilder before = null;
		// List.
		if (type == TYPE_ARRAY) {
			List<Object> ret = new ArrayList<Object>();
			int flg = 0;
			for (int i = no + 1; i < len; i++) {
				value = (String) token.get(i);
				if (",".equals(value) || "]".equals(value)) {
					if ("]".equals(value)) {
						if (flg == 1) {
							if (before != null) {
								ret.add(decJsonValue(n, i, before.toString()));
							}
						}
						n[0] = i;
						return ret;
					} else {
						if (flg == 1) {
							if (before == null) {
								ret.add(null);
							} else {
								ret.add(decJsonValue(n, i, before.toString()));
							}
						}
					}
					before = null;
					flg = 0;
				} else if ("[".equals(value)) {
					ret.add(createJsonInfo(mode, n, token, 0, i, len));
					i = n[0];
					before = null;
					flg = 0;
				} else if ("{".equals(value)) {
					ret.add(createJsonInfo(mode, n, token, 1, i, len));
					i = n[0];
					before = null;
					flg = 0;
				} else {
					if (before == null) {
						before = new StringBuilder();
						before.append(value);
					} else {
						before.append(" ").append(value);
					}
					flg = 1;
				}
			}
			n[0] = len - 1;
			return ret;
		}
		// map.
		else if (type == TYPE_MAP) {
			Map<String, Object> ret;
			if (mode) {
				ret = new ConcurrentHashMap<String, Object>();
			} else {
				ret = new HashMap<String, Object>();
			}
			String key = null;
			for (int i = no + 1; i < len; i++) {
				value = (String) token.get(i);
				if (":".equals(value)) {
					if (key == null) {
						throw new IOException("Map形式が不正です(No:" + i + ")");
					}
				} else if (",".equals(value) || "}".equals(value)) {
					if ("}".equals(value)) {
						if (key != null) {
							if (before == null) {
								ret.put(key, null);
							} else {
								ret.put(key, decJsonValue(n, i, before
										.toString()));
							}
						}
						n[0] = i;
						return ret;
					} else {
						if (key == null) {
							if (before == null) {
								continue;
							}
							throw new IOException("Map形式が不正です(No:" + i + ")");
						}
						if (before == null) {
							ret.put(key, null);
						} else {
							ret.put(key, decJsonValue(n, i, before.toString()));
						}
						before = null;
						key = null;
					}
				} else if ("[".equals(value)) {
					if (key == null) {
						throw new IOException("Map形式が不正です(No:" + i + ")");
					}
					ret.put(key, createJsonInfo(mode, n, token, 0, i, len));
					i = n[0];
					key = null;
					before = null;
				} else if ("{".equals(value)) {
					if (key == null) {
						throw new IOException("Map形式が不正です(No:" + i + ")");
					}
					ret.put(key, createJsonInfo(mode, n, token, 1, i, len));
					i = n[0];
					key = null;
					before = null;
				} else if (key == null) {
					key = value;
					if ((key.startsWith("'") && key.endsWith("'"))
							|| (key.startsWith("\"") && key.endsWith("\""))) {
						key = key.substring(1, key.length() - 1).trim();
					}
				} else {
					if (before == null) {
						before = new StringBuilder();
						before.append(value);
					} else {
						before.append(" ").append(value);
					}
				}
			}
			n[0] = len - 1;
			return ret;
		}
		// その他.
		throw new IOException("JSON解析に失敗");
	}
}
