package org.maachang.leveldb.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ディープ系処理.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public final class Deep {
	protected Deep() {
	}

	/**
	 * オブジェクトコピー.
	 * 
	 * @param obj
	 *            コピー元のオブジェクトを設定します.
	 * @return Object コピーされたオブジェクトが返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static Object cp(Object o) throws Exception {
		if (o == null) {
			return null;
		} else if (o instanceof Number) {

			if (o instanceof Byte || o instanceof Short || o instanceof Integer || o instanceof Long
					|| o instanceof Float || o instanceof Double) {
				return o;
			} else if (o instanceof AtomicInteger) {
				return new AtomicInteger(((AtomicInteger) o).intValue());
			} else if (o instanceof AtomicLong) {
				return new AtomicLong(((AtomicLong) o).longValue());
			} else if (o instanceof BigDecimal) {
				return new BigDecimal(o.toString());
			} else if (o instanceof BigInteger) {
				return new BigInteger(o.toString());
			}
		} else if (o instanceof String || o instanceof Boolean || o instanceof Character) {
			return o;
		} else if (o instanceof java.util.Date) {
			if (o instanceof java.sql.Date) {
				return new java.sql.Date(((java.util.Date) o).getTime());
			} else if (o instanceof java.sql.Time) {
				return new java.sql.Time(((java.util.Date) o).getTime());
			} else if (o instanceof java.sql.Timestamp) {
				return new java.sql.Timestamp(((java.util.Date) o).getTime());
			}
			return new java.util.Date(((java.util.Date) o).getTime());
		} else if (o instanceof List) {
			List ret = new ArrayList();
			List n = (List) o;
			int len = n.size();
			for (int i = 0; i < len; i++) {
				ret.add(cp(n.get(i)));
			}
			return ret;
		} else if (o instanceof Map) {
			Object key;
			Map ret = new HashMap();
			Map n = (Map) o;
			Iterator it = n.keySet().iterator();
			while (it.hasNext()) {
				key = it.next();
				ret.put(key, cp(n.get(key)));
			}
			return ret;
		} else if (o instanceof Set) {
			Object key;
			Set ret = new HashSet();
			Set n = (Set) o;
			Iterator it = n.iterator();
			while (it.hasNext()) {
				key = it.next();
				ret.add(cp(key));
			}
			return ret;
		} else if (o.getClass().isArray()) {
			if (o instanceof byte[]) {
				byte[] n = (byte[]) o;
				int len = n.length;
				byte[] ret = new byte[len];
				System.arraycopy(n, 0, ret, 0, len);
				return ret;
			} else if (o instanceof char[]) {
				char[] n = (char[]) o;
				int len = n.length;
				char[] ret = new char[len];
				System.arraycopy(n, 0, ret, 0, len);
				return ret;
			} else if (o instanceof Object[]) {
				Object[] n = (Object[]) o;
				int len = n.length;
				Object[] ret = new Object[len];
				for (int i = 0; i < len; i++) {
					ret[i] = cp(n[i]);
				}
				return ret;
			} else if (o instanceof int[]) {
				int[] n = (int[]) o;
				int len = n.length;
				int[] ret = new int[len];
				System.arraycopy(n, 0, ret, 0, len);
				return ret;
			} else if (o instanceof long[]) {
				long[] n = (long[]) o;
				int len = n.length;
				long[] ret = new long[len];
				System.arraycopy(n, 0, ret, 0, len);
				return ret;
			} else if (o instanceof String[]) {
				String[] n = (String[]) o;
				int len = n.length;
				String[] ret = new String[len];
				System.arraycopy(n, 0, ret, 0, len);
				return ret;
			} else if (o instanceof float[]) {
				float[] n = (float[]) o;
				int len = n.length;
				float[] ret = new float[len];
				System.arraycopy(n, 0, ret, 0, len);
				return ret;
			} else if (o instanceof double[]) {
				double[] n = (double[]) o;
				int len = n.length;
				double[] ret = new double[len];
				System.arraycopy(n, 0, ret, 0, len);
				return ret;
			} else if (o instanceof boolean[]) {
				boolean[] n = (boolean[]) o;
				int len = n.length;
				boolean[] ret = new boolean[len];
				System.arraycopy(n, 0, ret, 0, len);
				return ret;
			} else if (o instanceof short[]) {
				short[] n = (short[]) o;
				int len = n.length;
				short[] ret = new short[len];
				System.arraycopy(n, 0, ret, 0, len);
				return ret;
			} else {
				int len = Array.getLength(o);
				Object ret = Array.newInstance(o.getClass(), len);
				for (int i = 0; i < len; i++) {
					Array.set(ret, i, cp(Array.get(o, i)));
				}
				return ret;
			}
		} else if (o instanceof Serializable) {
			// シリアライズ可能な内容はシリアライズしてコピー.
			ByteArrayOutputStream baos = null;
			ObjectOutputStream oos = null;
			ObjectInputStream ois = null;
			Object ret = null;
			try {
				baos = new ByteArrayOutputStream();
				oos = new ObjectOutputStream(baos);
				oos.writeObject(o);
				oos.flush();
				ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
				ret = ois.readObject();
				oos.close();
				ois.close();
				oos = null;
				ois = null;
			} catch (Exception e) {
				throw new IOException("オブジェクト[" + o.getClass().getName() + "]のコピーに失敗しました", e);
			} finally {
				if (oos != null) {
					try {
						oos.close();
					} catch (Exception e) {
					}
				}
				if (ois != null) {
					try {
						ois.close();
					} catch (Exception e) {
					}
				}
			}
			return ret;
		}
		throw new IOException("オブジェクト[" + o.getClass().getName() + "]のコピーに失敗しました");
	}

	/**
	 * オブジェクト一致チェック. 厳密にチェックします.
	 * 
	 * @param src
	 *            チェック元のオブジェクトを設定します.
	 * @param dest
	 *            チェック先のオブジェクトを設定します.
	 * @return boolean [true]の場合、一致しています.
	 */
	public static final boolean eq(Object src, Object dest) {
		if (src == dest) {
			return true;
		} else if (src == null || dest == null) {
			return false;
		} else if (src.equals(dest)) {
			return true;
		} else if (src instanceof List && dest instanceof List) {
			List s = (List) src;
			List d = (List) dest;
			int len = s.size();
			if (len != d.size()) {
				return false;
			}
			for (int i = 0; i < len; i++) {
				if (!eq(s.get(i), d.get(i))) {
					return false;
				}
			}
			return true;
		} else if (src instanceof Map && dest instanceof Map) {
			Map s = (Map) src;
			Map d = (Map) dest;
			int len = s.size();
			if (len != d.size()) {
				return false;
			}
			Object key;
			Iterator it = s.keySet().iterator();
			while (it.hasNext()) {
				key = it.next();
				if (!eq(s.get(key), d.get(key))) {
					return false;
				}
			}
			return true;
		} else if (src instanceof Set && dest instanceof Set) {
			Set s = (Set) src;
			Set d = (Set) dest;
			int len = s.size();
			if (len != d.size()) {
				return false;
			}
			Object key;
			Iterator it = s.iterator();
			while (it.hasNext()) {
				key = it.next();
				if (!d.contains(key)) {
					return false;
				}
			}
			return true;
		} else if (src.getClass().isArray() && dest.getClass().isArray()) {
			int len = Array.getLength(src);
			if (len != Array.getLength(dest)) {
				return false;
			}
			for (int i = 0; i < len; i++) {
				if (!eq(Array.get(src, i), Array.get(dest, i))) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

}
