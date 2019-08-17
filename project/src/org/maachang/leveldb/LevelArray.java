package org.maachang.leveldb;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import org.maachang.leveldb.util.Utils;

/**
 * Leveldb配列オブジェクト. 読み込みに特化した、MapライクなLevedb専用の配列オブジェクト.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class LevelArray {

	/** 配列追加長. **/
	protected static final int ADD_LEN = 4;

	/** 近似値範囲. **/
	private static final int SEARCH_RANGE = 5;

	/**
	 * キー情報管理. キー情報の１つの配列は以下の２配列で構成. [0] Object キーコード. [1] int データポジション位置.
	 * 
	 * これらの値が、ソートされて保持される.
	 */
	protected Object[] keyList;

	/** データ情報. **/
	protected Object[] dataList;

	/** 有効データ長. **/
	protected int length;

	/** 最終ステータス. **/
	protected int lastState = -2;

	/**
	 * コンストラクタ.
	 */
	public LevelArray() {

	}

	/**
	 * コンストラクタ.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param value
	 *            対象の要素を設定します.
	 */
	public LevelArray(Object key, Object value) {
		keyList = new Object[ADD_LEN + 1];
		keyList[0] = new Object[] { convertKey(key), 0 };
		dataList = new Object[ADD_LEN + 1];
		dataList[0] = value;
		length = 1;
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param args
	 *            [key,value,key,value...]と交互に設定します.
	 * @return LevelArray オブジェクトが返却されます.
	 */
	public LevelArray(Object... args) {
		putAll(args);
	}

	/**
	 * 情報のクリア.
	 */
	public final void clear() {
		keyList = null;
		dataList = null;
		length = 0;
		lastState = -2;
	}

	/**
	 * put系最終処理ステータスを取得.
	 * 
	 * @return int [-2]が返却された場合、処理は行われていません. [-1]が返却された場合、引数の問題等で、処理が行われていません.
	 *         [0]が返却された場合、処理結果の件数が存在しません. [1以上]が返却された場合、正常終了しています.
	 */
	public final int getLastState() {
		return lastState;
	}

	/**
	 * データセット.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param value
	 *            対象の要素を設定します.
	 * @return LevelArray オブジェクトが返却されます.
	 */
	public final LevelArray put(Object key, Object value) {
		if (notKey(key)) {

			// 未引数終了.
			lastState = -1;
			return this;
		}
		// データが存在しない場合.
		else if (length == 0) {
			keyList = new Object[ADD_LEN + 1];
			keyList[0] = new Object[] { convertKey(key), 0 };
			dataList = new Object[ADD_LEN + 1];
			dataList[0] = value;
			length = 1;

			// 正常終了.
			lastState = 1;
			return this;
		}
		int len = length;
		key = convertKey(key);

		// 同一データ検索.
		int p = searchKey(keyList, len, key);

		// 検索結果が見つからない場合.
		if (p == -1) {

			// キーリストの追加位置を特定.
			int off = 0;
			if (len > SEARCH_RANGE) {
				int n = len >> 1;
				while (n > SEARCH_RANGE) {
					if ((p = compareTo(key, ((Object[]) keyList[off + n])[0])) > 0) {
						off += n;
						if (off + 1 >= len) {
							off = len - 1;
							break;
						}
					} else if (p < 0) {
						off -= n;
						if (off <= 0) {
							off = 0;
							break;
						}
					}
					n = n >> 1;
				}
			}

			// データ領域を増やす必要がある場合.
			Object[] v, vv;
			if (keyList.length <= length + 1) {
				v = new Object[length + ADD_LEN];
				vv = new Object[length + ADD_LEN];
			}
			// データ領域を増やさなくてよい場合.
			else {
				v = keyList;
				vv = dataList;
			}

			// ソート追加.
			boolean sort = false;
			for (int i = off; i < len; i++) {
				if (compareTo(key, ((Object[]) keyList[i])[0]) <= 0) {
					if (v != keyList) {
						System.arraycopy(keyList, 0, v, 0, i);
						System.arraycopy(keyList, i, v, i + 1, len - i);
						keyList = v;
					} else {
						System.arraycopy(keyList, i, v, i + 1, len - i);
					}
					v[i] = new Object[] { key, len };
					sort = true;
					break;
				}
			}
			// ソート追加できない場合は、最後に追加.
			if (!sort) {
				if (v != keyList) {
					System.arraycopy(keyList, 0, v, 0, len);
					keyList = v;
				}
				v[len] = new Object[] { key, len };
			}

			// データリストに追加.
			if (vv != dataList) {
				System.arraycopy(dataList, 0, vv, 0, len);
				dataList = vv;
			}
			vv[len] = value;
			length++;

			// 正常終了.
			lastState = 1;
			return this;
		}

		// 検索結果が見つかった場合は、データ領域を更新.
		dataList[(Integer) (((Object[]) keyList[p])[1])] = value;

		// 正常終了.
		lastState = 1;
		return this;
	}

	/**
	 * 更新セット. この処理では、既に存在する条件に対して、データセットします.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param value
	 *            対象の要素を設定します.
	 * @return LevelArray オブジェクトが返却されます.
	 */
	public final LevelArray update(Object key, Object value) {
		if (notKey(key) || length == 0) {

			// 未引数終了.
			lastState = -1;
			return this;
		}
		lastState = 0;
		int p = searchKey(keyList, length, convertKey(key));
		if (p != -1) {
			dataList[(Integer) (((Object[]) keyList[p])[1])] = value;
			lastState = 1;
		}

		return this;
	}

	/**
	 * データセット.
	 * 
	 * @param args
	 *            [key,value,key,value...]と交互に設定します.
	 * @return LevelArray オブジェクトが返却されます.
	 */
	public final LevelArray putAll(Object... args) {
		if (args == null || args.length == 0) {

			// 未引数終了.
			lastState = -1;
			return this;
		}
		int p;
		Object k;
		int cnt = 0;
		int len = args.length;
		_addCapacity(len >> 1);
		for (int i = 0; i < len; i += 2) {
			if (!notKey(args[i])) {

				// データが存在しない場合は、新規追加.
				k = convertKey(args[i]);
				if ((p = indexOf(keyList, length, k)) == -1) {
					keyList[length] = new Object[] { k, length };
					dataList[length] = args[i + 1];
					length++;
					cnt++;
				}
				// データが存在する場合は上書き.
				else {
					dataList[(Integer) ((Object[]) keyList)[p]] = args[i + 1];
				}
			}
		}
		if (cnt > 0) {
			keySort(keyList);
		}

		// 成功処理数をセット.
		lastState = cnt;
		return this;
	}

	/**
	 * 更新セット. この処理では、既に存在する条件に対して、データセットします.
	 * 
	 * @param args
	 *            [key,value,key,value...]と交互に設定します.
	 * @return LevelArray オブジェクトが返却されます.
	 */
	public final LevelArray updateAll(Object... args) {
		if (args == null || args.length == 0) {

			// 未引数終了.
			lastState = -1;
			return this;
		}
		int p;
		int cnt = 0;
		int len = args.length;
		for (int i = 0; i < len; i += 2) {
			if (!notKey(args[i])) {

				p = searchKey(keyList, length, convertKey(args[i]));
				if (p != -1) {
					dataList[(Integer) (((Object[]) keyList[p])[1])] = args[i + 1];
					cnt++;
				}
			}
		}

		// 成功処理数をセット.
		lastState = cnt;
		return this;
	}

	/**
	 * データのマージ.
	 * 
	 * @param v
	 *            マージ対象のLevelArrayを設定します.
	 * @return LevelArray オブジェクトが返却されます.
	 */
	public final LevelArray marge(LevelArray v) {
		return marge(true, v);
	}

	/**
	 * データのマージ.
	 * 
	 * @param mode
	 *            [true]を設定した場合、対象情報が存在しない場合は、新規追加します.
	 *            [false]の場合は、現存する内容と一致するものを更新します.
	 * @param v
	 *            マージ対象のLevelArrayを設定します.
	 * @return LevelArray オブジェクトが返却されます.
	 */
	public final LevelArray marge(boolean mode, LevelArray v) {
		if (v == null || v.size() == 0) {

			// 未引数終了.
			lastState = -1;
			return this;
		}
		int len = v.size();
		_addCapacity(len);

		int p;
		int cnt = 0;
		Object[] o;
		Object[] a = v.keyList;
		Object[] b = v.dataList;
		for (int i = 0; i < len; i++) {
			o = (Object[]) a[i];

			// データが存在する場合は、上書き.
			if ((p = indexOf(keyList, length, o[0])) != -1) {
				dataList[(Integer) ((Object[]) keyList[p])[1]] = b[(Integer) o[1]];
			}
			// データが存在しない場合は、新規追加.
			// ただし、モードが[true]のときのみ.
			else if (mode) {
				keyList[length] = new Object[] { o[0], length };
				dataList[length] = b[(Integer) o[1]];
				length++;
				cnt++;
			}
		}
		if (cnt > 0) {
			keySort(keyList);
		}

		// 成功処理数をセット.
		lastState = cnt;
		return this;
	}

	/**
	 * データのマージ.
	 * 
	 * @param v
	 *            マージ対象のMapオブジェクトを設定します.
	 * @return LevelArray オブジェクトが返却されます.
	 */
	public final LevelArray marge(Map v) {
		return marge(true, v);
	}

	/**
	 * データのマージ.
	 * 
	 * @param mode
	 *            [true]を設定した場合、対象情報が存在しない場合は、新規追加します.
	 *            [false]の場合は、現存する内容と一致するものを更新します.
	 * @param v
	 *            マージ対象のMapオブジェクトを設定します.
	 * @return LevelArray オブジェクトが返却されます.
	 */
	public final LevelArray marge(boolean mode, Map v) {
		if (v == null || v.size() == 0) {

			// 未引数終了.
			lastState = -1;
			return this;
		}
		_addCapacity(v.size());

		int p;
		Object k, kk;
		int cnt = 0;
		Iterator it = v.keySet().iterator();
		while (it.hasNext()) {

			// NULL以外のキーで、配列オブジェトでない場合は処理.
			if (!notKey(k = it.next())) {

				// データが存在する場合は、上書き.
				kk = convertKey(k);
				if ((p = indexOf(keyList, length, kk)) != -1) {
					dataList[(Integer) ((Object[]) keyList[p])[1]] = v.get(k);
				}
				// データが存在しない場合は、新規追加.
				// ただし、モードが[true]のときのみ.
				else if (mode) {
					keyList[length] = new Object[] { kk, length };
					dataList[length] = v.get(k);
					length++;
					cnt++;
				}
			}
		}
		if (cnt > 0) {
			keySort(keyList);
		}

		// 成功処理数をセット.
		lastState = cnt;
		return this;
	}

	/**
	 * データのマージ.
	 * 
	 * @param v
	 *            マージ対象のオブジェクトを設定します. このオブジェクトはLevelArrayか、Mapか、
	 *            v.getClass().isArray()==trueである必要があります.
	 * @return LevelArray オブジェクトが返却されます.
	 */
	public final LevelArray marge(Object v) {
		return marge(true, v);
	}

	/**
	 * データのセット.
	 * 
	 * @param mode
	 *            [true]を設定した場合、対象情報が存在しない場合は、新規追加します.
	 *            [false]の場合は、現存する内容と一致するものを更新します.
	 * @param v
	 *            マージ対象のオブジェクトを設定します. このオブジェクトはLevelArrayか、Mapか、
	 *            v.getClass().isArray()==trueである必要があります.
	 * @return LevelArray オブジェクトが返却されます.
	 */
	public final LevelArray marge(boolean mode, Object v) {
		if (v == null) {

			// 未引数終了.
			lastState = -1;
			return this;
		} else if (v instanceof LevelArray) {
			return marge(mode, (LevelArray) v);
		} else if (v instanceof Map) {
			return marge(mode, (Map) v);
		} else if (!v.getClass().isArray()) {

			// 未引数終了.
			lastState = -1;
			return this;
		}

		int p;
		Object o;
		int cnt = 0;
		int len = Array.getLength(v);
		for (int i = 0; i < len; i += 2) {
			o = Array.get(v, i);

			// NULLでなく、配列でないキーのみ処理.
			if (!notKey(o)) {

				// データが存在する場合は、上書き.
				o = convertKey(o);
				if ((p = indexOf(keyList, length, o)) != -1) {
					dataList[(Integer) ((Object[]) keyList[p])[1]] = Array.get(
							v, i + 1);
				}
				// データが存在しない場合は、新規追加.
				// ただし、モードが[true]のときのみ.
				else if (mode) {
					keyList[length] = new Object[] { o, length };
					dataList[length] = Array.get(v, i + 1);
					length++;
					cnt++;
				}
			}
		}
		if (cnt > 0) {
			keySort(keyList);
		}

		// 成功処理数をセット.
		lastState = cnt;
		return this;
	}

	/**
	 * 情報存在確認.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return boolean [true]の場合、存在します.
	 */
	public final boolean contains(Object key) {
		if (notKey(key) || length == 0) {
			return false;
		}
		return searchKey(keyList, length, convertKey(key)) != -1;
	}

	/**
	 * 情報取得.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return Object 情報が返却されます.
	 */
	public final Object get(Object key) {
		if (notKey(key) || length == 0) {
			return null;
		}
		int p = searchKey(keyList, length, convertKey(key));
		if (p == -1) {
			return null;
		}
		return dataList[(Integer) (((Object[]) keyList[p])[1])];
	}

	/**
	 * 削除処理.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @return Object 削除された情報が返却されます.
	 */
	public final Object remove(Object key) {
		if (notKey(key) || length == 0) {
			return null;
		}
		int p = searchKey(keyList, length, convertKey(key));
		if (p == -1) {
			return null;
		}
		return removeData(p);
	}

	/**
	 * データ件数を取得.
	 * 
	 * @return int データ件数が返却されます.
	 */
	public final int size() {
		return length;
	}

	/**
	 * キー情報を番号指定で取得.
	 * 
	 * @param no
	 *            対象の項番を設定します.
	 * @return Object キー情報が返却されます.
	 */
	public final Object getKey(int no) {
		if (length == 0 || no < 0 || no >= length) {
			return null;
		}
		return (((Object[]) keyList[no])[0]);
	}

	/**
	 * データ情報を番号指定で取得.
	 * 
	 * @param no
	 *            対象の項番を設定します.
	 * @return Object データ情報が返却されます.
	 */
	public final Object getData(int no) {
		if (length == 0 || no < 0 || no >= length) {
			return null;
		}
		return dataList[(Integer) (((Object[]) keyList[no])[1])];
	}

	/**
	 * 指定番号のデータを削除.
	 * 
	 * @param no
	 *            対象の項番を設定します.
	 * @return Object 削除されたデータが返却されます.
	 */
	public final Object removeData(int no) {
		if (length == 0 || no < 0 || no >= length) {
			return null;
		}
		int len = length;
		length--;
		int dataNo = (Integer) (((Object[]) keyList[no])[1]);
		Object ret = dataList[dataNo];

		// データがゼロ件出ない場合.
		if (length != 0) {

			// キーリストを削除.
			System.arraycopy(keyList, 0, keyList, 0, no);
			System.arraycopy(keyList, no + 1, keyList, no, len - (no + 1));

			// データリストを削除.
			System.arraycopy(dataList, 0, dataList, 0, dataNo);
			System.arraycopy(dataList, dataNo + 1, dataList, dataNo, len
					- (dataNo + 1));

			// キーリストのポジションを再定義.
			if (dataNo < length) {
				int t;
				Object[] o;
				for (int i = 0; i < length; i++) {
					o = (Object[]) keyList[i];
					t = (Integer) o[1];
					if (t > dataNo) {
						o[1] = t - 1;
					}
				}
			}
		}

		// データクリア.
		keyList[length] = null;
		dataList[length] = null;

		return ret;
	}

	/** データ容量の増加. **/
	private final void _addCapacity(int len) {
		if (length == 0) {
			keyList = new Object[len + ADD_LEN];
			dataList = new Object[len + ADD_LEN];
		} else if (length + len >= keyList.length) {
			Object[] o = new Object[length + len + ADD_LEN];
			System.arraycopy(keyList, 0, o, 0, length);
			keyList = o;

			o = new Object[length + len + ADD_LEN];
			System.arraycopy(dataList, 0, o, 0, length);
			dataList = o;
		}
	}

	/** キー変換. **/
	private static final Object convertKey(Object key) {
		if (Utils.isNumeric(key)) {
			return Utils.convertLong(key);
		}
		return Utils.convertString(key);
	}

	/** 判別処理. **/
	private static final int compareTo(Object a, Object b) {
		if (a instanceof Long && b instanceof Long) {
			return ((Comparable) a).compareTo(b);
		}
		return Utils.convertString(a).compareTo(Utils.convertString(b));
	}

	/** ソート用オブジェクト. **/
	private static final class _KeySort implements Comparator<Object> {
		public final int compare(Object o1, Object o2) {

			// 範囲外の条件は下にソート.
			if (o1 == o2)
				return 0;
			if (o1 == null)
				return 1;
			if (o2 == null)
				return -1;

			// nullで無い場合はソート処理.
			return compareTo(((Object[]) o1)[0], ((Object[]) o2)[0]);
		}
	}

	/** データソート用オブジェクト. **/
	private static final _KeySort _sortObject = new _KeySort();

	/** データソート. **/
	private static final void keySort(Object[] o) {
		Arrays.sort(o, _sortObject);
	}

	/** キー検索. **/
	private static final int searchKey(Object[] k, int len, Object key) {
		// データがソート済みの場合に利用する.

		int high = len - 1;
		int low = 0;
		int mid, cmp;
		while (low <= high) {
			mid = (low + high) >>> 1;
			if ((cmp = compareTo(((Object[]) k[mid])[0], key)) < 0) {
				low = mid + 1;
			} else if (cmp > 0) {
				high = mid - 1;
			} else {
				return mid;
			}
		}
		return -1;
	}

	/** 順番にキー検索. **/
	private static final int indexOf(Object[] k, int len, Object key) {
		// データがソートされていない場合に利用する.

		for (int i = 0; i < len; i++) {
			if (compareTo(((Object[]) k[i])[0], key) == 0) {
				return i;
			}
		}
		return -1;
	}

	/** キーチェック. **/
	private static final boolean notKey(Object key) {
		return key == null || key.getClass().isArray();
	}
}
