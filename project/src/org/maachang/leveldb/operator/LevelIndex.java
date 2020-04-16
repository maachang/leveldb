package org.maachang.leveldb.operator;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.LevelBuffer;
import org.maachang.leveldb.LevelId;
import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.LevelValues;
import org.maachang.leveldb.Leveldb;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.LeveldbIterator;
import org.maachang.leveldb.types.TwoKey;
import org.maachang.leveldb.util.Alphabet;
import org.maachang.leveldb.util.BinaryUtil;
import org.maachang.leveldb.util.Converter;
import org.maachang.leveldb.util.ObjectList;

/**
 * Levelインデックス.
 */
public class LevelIndex extends LevelOperator {
	/** インデックスカラムタイプ: 文字列. **/
	public static final int INDEX_STRING = 0;
	/** インデックスカラムタイプ: ３２ビット数値. **/
	public static final int INDEX_INT = 1;
	/** インデックスカラムタイプ: ６４ビット数値. **/
	public static final int INDEX_LONG = 2;
	/** インデックスカラムタイプ: ３２ビット浮動小数点. **/
	public static final int INDEX_FLOAT = 3;
	/** インデックスカラムタイプ: ６４ビット浮動小数点. **/
	public static final int INDEX_DOUBLE = 4;
	/** インデックスカラムタイプ: バイナリ. **/
	public static final int INDEX_BINARY = 5;
	
	// インデックスタイプ文字列パターン.
	private static final String[] PATTERN_STR = new String[] { "str", "string", "char" };
	private static final String[] PATTERN_INT = new String[] { "n32", "int", "integer", "number32" };
	private static final String[] PATTERN_LONG = new String[] { "n64", "long", "number64", "bigint" };
	private static final String[] PATTERN_FLOAT = new String[] { "float", "decimal32", "dec32", "float32" };
	private static final String[] PATTERN_DOUBLE = new String[] { "double", "decimal64", "dec64", "float64" };
	private static final String[] PATTERN_BINARY = new String[] { "binary", "bin" };
	private static final Object[] PATTERNS = new Object[] {
		PATTERN_STR, PATTERN_INT, PATTERN_LONG, PATTERN_FLOAT, PATTERN_DOUBLE, PATTERN_BINARY
	};
	
	// Leveldbインデックス名拡張子.
	protected static final String INDEX_FOODER = ".idx";
	protected static final String INDEX_CUT = "'";
	protected static final int MAX_ERROR = 32;
	
	protected Leveldb parent; // インデックス元のLeveldbオブジェクト.
	protected int parentType; // インデックス元のLeveldbキータイプ.
	protected int indexColumnType; // インデックスカラムタイプ.
	protected int indexColumnLvType; // インデックスカラムのLeveldb用タイプ.
	protected int indexKeyType; // インデックスのカラムタイプ.
	protected String indexColumnName; // インデックスカラム名.
	protected String[] indexColumnList; // インデックスカラム名(hoge.moge.abcのように階層設定可能).
	
	/**
	 * オペレータタイプ.
	 * @return int オペレータタイプが返却されます.
	 */
	@Override
	public int getOperatorType() {
		return LEVEL_INDEX;
	}
	
	// カラム名を分解.
	protected static final String[] columnNames(String columnName) {
		if(columnName == null || columnName.isEmpty()) {
			return null;
		}
		final ObjectList<String> cnames = new ObjectList<String>();
		Converter.cutString(cnames, true, columnName, ".");
		Object[] o = cnames.rawData().toArray();
		int len = cnames.size();
		String[] ret = new String[len];
		System.arraycopy(o, 0, ret, 0, len);
		return ret;
	}
	
	// カラム名を文字列に戻す.
	public static final String srcColumnNames(String[] list) {
		if(list == null || list.length == 0) {
			return null;
		}
		StringBuilder buf = new StringBuilder();
		int len = list.length;
		for(int i = 0; i < len; i ++) {
			if(i != 0) {
				buf.append(".");
			}
			buf.append(list[i]);
		}
		return buf.toString();
	}
	
	/**
	 * 文字列で設定されたカラムタイプを数値変換.
	 * @param name
	 * @return
	 */
	public static final int convertStringByColumnType(final String name) {
		int j, lenJ;
		String[] target;
		final int len = PATTERNS.length;
		for(int i = 0; i < len; i ++) {
			target = (String[])PATTERNS[i];
			lenJ = target.length;
			for (j = 0; j < lenJ; j++) {
				if (Alphabet.eq(name, target[j])) {
					return i;
				}
			}
		}
		return -1;
	}
	
	// インデックスカラムタイプをLevelOptionのタイプに変換.
	private static final int convertColumTypeByLevelOptionType(final int type) {
		switch(type) {
		case INDEX_STRING: return LevelOption.TYPE_STRING;
		case INDEX_INT: return LevelOption.TYPE_NUMBER32;
		case INDEX_LONG: return LevelOption.TYPE_NUMBER64;
		case INDEX_FLOAT: return LevelOption.TYPE_NUMBER32;
		case INDEX_DOUBLE: return LevelOption.TYPE_NUMBER64;
		case INDEX_BINARY: return LevelOption.TYPE_FREE;
		}
		throw new LeveldbException("Unknown index column type: " + type);
	}
	
	// インデックスカラムを変換.
	private static final Object convertColumType(final int type, final Object o) {
		if(o == null) {
			return null;
		}
		switch(type) {
		case INDEX_STRING:
			return Converter.convertString(o);
		case INDEX_INT:
			if(Converter.isNumeric(o)) {
				return Converter.convertInt(o);
			}
			return null;
		case INDEX_LONG:
			if(Converter.isNumeric(o)) {
				return Converter.convertLong(o);
			}
			return null;
		case INDEX_FLOAT:
			if(Converter.isNumeric(o)) {
				return Float.floatToRawIntBits(Converter.convertFloat(o));
			}
			return null;
		case INDEX_DOUBLE:
			if(Converter.isNumeric(o)) {
				return Double.doubleToRawLongBits(Converter.convertDouble(o));
			}
			return null;
		case INDEX_BINARY:
			if(o instanceof byte[]) {
				return (byte[])o;
			}
			return null;
		}
		throw new LeveldbException("Unknown index column type: " + type);
	}
	
	/**
	 * コンストラクタ.
	 * Writebatch無効で作成.
	 * カラム名は hoge.moge.abc のように '.' 区切りにすることで、階層で設定できます.
	 * 
	 * @param columnType インデックスカラムタイプ.
	 * @param columnName インデクスカラム名(hoge.moge.abcのように階層設定可能).
	 * @param parent インデックス元のLeveldbオブジェクト.
	 */
	public LevelIndex(String columnType, String columnName, Leveldb parent) {
		this(convertStringByColumnType(columnType) , columnName, parent);
	}
	
	/**
	 * コンストラクタ.
	 * Writebatch無効で作成.
	 * カラム名は hoge.moge.abc のように '.' 区切りにすることで、階層で設定できます.
	 * 
	 * @param columnType インデックスカラムタイプ.
	 * @param columnName インデクスカラム名(hoge.moge.abcのように階層設定可能).
	 * @param parent インデックス元のLeveldbオブジェクト.
	 */
	public LevelIndex(int columnType, String columnName, Leveldb parent) {
		int indexColumnLvType = convertColumTypeByLevelOptionType(columnType);
		String[] list = columnNames(columnName);
		columnName = srcColumnNames(list);
		String columnString = LevelOption.stringType(indexColumnLvType);
		int indexKeyType = LevelOption.convertType(columnString + "-" + "binary");
		LevelOption pOpt = parent.getOption();
		LevelOption opt = LevelOption.create(
			indexKeyType,
			pOpt.getWriteBufferSize(),
			pOpt.getMaxOpenFiles(),
			pOpt.getBlockSize(),
			pOpt.getBlockCache());
		Leveldb db = new Leveldb(new StringBuilder(parent.getPath())
			.append(INDEX_CUT)
			.append(columnName)
			.append(INDEX_CUT)
			.append(columnType)
			.append(INDEX_FOODER)
			.toString(), opt);
		
		// leveldbをクローズしてwriteBatchで処理しない.
		super.init(null, db, true, false);
		
		this.parent = parent;
		this.parentType = pOpt.getType();
		this.indexColumnType = columnType;
		this.indexColumnLvType = indexColumnLvType;
		this.indexKeyType = indexKeyType;
		this.indexColumnList = list;
		this.indexColumnName = columnName;
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを有効にして生成します.
	 * 
	 * @param idx 親となるオペレータを設定します.
	 */
	public LevelIndex(LevelIndex idx) {
		// leveldbをクローズせずwriteBatchで処理する.
		super.init(idx, idx.leveldb, false, true);
		this.parent = idx.leveldb;
		this.parentType = idx.parentType;
		this.indexColumnType = idx.indexColumnType;
		this.indexColumnLvType = idx.indexColumnLvType;
		this.indexKeyType = idx.indexKeyType;
		this.indexColumnList = idx.indexColumnList;
		this.indexColumnName = idx.indexColumnName;
	}
	
	/**
	 * インデックス対象のカラムタイプを取得.
	 * @return
	 */
	public int getColumnType() {
		checkClose();
		return indexColumnType;
	}
	
	/**
	 * インデックス対象のカラム名を取得.
	 * @return
	 */
	public String getColumnName() {
		checkClose();
		return indexColumnName;
	}
	
	// キーバイナリを取得.
	private static final byte[] keyBinary(int type, Object key1, Object key2) {
		JniBuffer keyBuf = null;
		try {
			keyBuf = LevelBuffer.key(type, key1, key2);
			byte[] ret = keyBuf.getBinary();
			return ret;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, null);
		}
	}
	
	// valueがMapの場合、カラム名の情報を取得する.
	@SuppressWarnings("rawtypes")
	private static final Object getValueInColumns(String[] columnNames, Object value) {
		// valueがMapじゃない場合はインデックス化しない.
		// もしくは、指定カラム名が存在しない場合は、インデックス化しない.
		int len = columnNames.length;
		for(int i = 0; i < len; i ++) {
			if(!(value instanceof Map) ||
				(value = ((Map)value).get(columnNames[i])) == null) {
				return null;
			}
		}
		return value;
	}
	
	/**
	 * 指定キーの情報をセット.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param value
	 *            対象の要素を設定します.
	 *            この条件は、mapで columnNameの内容が設定されている必要があります.
	 * @return boolean
	 *            [true]の場合、設定できました.
	 */
	public boolean put(Object key, Object value) {
		return put(key, null, value);
	}

	/**
	 * 指定キーの情報をセット.
	 * 
	 * @param value
	 *            対象の要素を設定します.
	 *            この条件は、mapで columnNameの内容が設定されている必要があります.
	 * @param key
	 *            対象のキー群を設定します.
	 * @return boolean
	 *            [true]の場合、設定できました.
	 */
	public boolean putMultiKey(Object value, Object... keys) {
		if (parentType != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb definition key type is not multi-key.");
		}
		return put(keys, null, value);
	}
	
	/**
	 * 指定キーの情報をセット.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param twoKey
	 *            対象のセカンドキーを設定します.
	 * @param value
	 *            対象の要素を設定します.
	 *            この条件は、mapで columnNameの内容が設定されている必要があります.
	 * @return boolean
	 *            [true]の場合、設定できました.
	 */
	public boolean put(Object key, Object twoKey, Object value) {
		checkClose();
		// valueがMapじゃない場合はインデックス化しない.
		Object o = getValueInColumns(indexColumnList, value);
		if(o == null) {
			return false;
		}
		return putDirectValue(key, twoKey, o);
	}
	
	/**
	 * インデックスカラムを直接指定して、追加.
	 * @param key
	 * @param twoKey
	 * @param value
	 * @return
	 */
	protected boolean putDirectValue(Object key, Object twoKey, Object value) {
		checkClose();
		// インデックスカラムがnullの場合はインデックス化しない.
		Object columnValue = convertColumType(indexColumnType, value);
		if(columnValue == null) {
			return false;
		}
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			// keyをバイナリ変換して、indexKeyとして[column, binary]の２キーをキーとする.
			// valueには、keyをバイナリ変換したものをセットする.
			byte[] keyBin = keyBinary(parentType, key, twoKey);
			keyBuf = LevelBuffer.key(indexKeyType, columnValue, keyBin);
			valBuf = LevelBuffer.value();
			valBuf.setBinary(keyBin);
			if(writeBatchFlag) {
				writeBatch().put(keyBuf, valBuf);
			} else {
				leveldb.put(keyBuf, valBuf);
			}
			return true;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, valBuf);
		}
	}
	
	
	/**
	 * 指定キーの情報を削除.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param value
	 *            削除対象の要素を設定します.
	 * @return boolean
	 *            削除できた場合[true]が返却されます.
	 */
	public boolean remove(Object key, Object value) {
		return remove(key, null, value);
	}

	/**
	 * 指定キーの情報を削除.
	 * 
	 * @param value
	 *            削除対象の要素を設定します.
	 * @param key
	 *            対象のキー群を設定します.
	 * @return boolean
	 *            削除できた場合[true]が返却されます.
	 */
	public boolean removeMultiKey(Object value, Object... keys) {
		if (parentType != LevelOption.TYPE_MULTI) {
			throw new LeveldbException("Leveldb definition key type is not multi-key.");
		}
		return remove(keys, null, value);
	}
	
	/**
	 * 指定キーの情報を削除.
	 * 
	 * @param key
	 *            対象のキーを設定します.
	 * @param twoKey
	 *            対象のセカンドキーを設定します.
	 * @paramn value
	 *            削除対象の要素を設定します.
	 * @return boolean
	 *            削除できた場合[true]が返却されます.
	 */
	public boolean remove(Object key, Object twoKey, Object value) {
		checkClose();
		// valueがMapじゃない場合はインデックス化しない.
		Object o = getValueInColumns(indexColumnList, value);
		if(o == null) {
			return false;
		}
		return removeDirectValue(key, twoKey, o);
	}
	
	/**
	 * インデックスカラムを直接指定して、削除.
	 * @param key
	 * @param twoKey
	 * @param value
	 * @return
	 */
	protected boolean removeDirectValue(Object key, Object twoKey, Object value) {
		checkClose();
		// インデックスカラムがnullの場合はインデックス化しない.
		Object columnValue = convertColumType(indexColumnType, value);
		if(columnValue == null) {
			return false;
		}
		JniBuffer keyBuf = null;
		try {
			// keyをバイナリ変換して、indexKeyとして[column, binary]の２キーをキーとする.
			byte[] keyBin = keyBinary(parentType, key, twoKey);
			keyBuf = LevelBuffer.key(indexKeyType, columnValue, keyBin);
			if(writeBatchFlag) {
				writeBatch().remove(keyBuf);
			} else {
				return leveldb.remove(keyBuf);
			}
			return true;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, null);
		}
	}
	
	/**
	 * インデックスカラムをソート順で取得.
	 * @return
	 */
	public LevelIndexIterator getSortIndex() {
		return _iterator(false, null);
	}
	
	/**
	 * インデックスカラムをソート順で取得.
	 * @param reverse
	 * @return
	 */
	public LevelIndexIterator getSortIndex(boolean reverse) {
		return _iterator(reverse, null);
	}
	
	/**
	 * インデックスカラムを直接指定して、情報取得.
	 * @param columnValue
	 * @return
	 */
	public LevelIndexIterator get(Object columnValue) {
		return _iterator(false, columnValue);
	}
	
	/**
	 * インデックスカラムを直接指定して、情報取得.
	 * @param reverse
	 * @param columnValue
	 * @return
	 */
	public LevelIndexIterator get(boolean reverse, Object columnValue) {
		return _iterator(reverse, columnValue);
	}
	
	// イテレータを取得.
	protected LevelIndexIterator _iterator(boolean reverse, Object value) {
		checkClose();
		LevelIndexIterator ret = null;
		try {
			Object columnValue = convertColumType(indexColumnType, value);
			ret = new LevelIndexIterator(reverse, this, leveldb.snapshot());
			if(columnValue != null) {
				return _search(ret, columnValue);
			} else if(reverse) {
				ret.itr.last();
			}
			return ret;
		} catch(LeveldbException le) {
			if(ret != null) {
				ret.close();
			}
			throw le;
		} catch(Exception e) {
			if(ret != null) {
				ret.close();
			}
			throw new LeveldbException(e);
		}
	}
	
	// ゼロバイナリ.
	private static final byte[] MIN_BIN = new byte[] {
		//(byte)0
	};
	
	// 最大データ長バイナリ
	private static final byte[] MAX_BIN = new byte[]{
		(byte)255, (byte)255, (byte)255, (byte)255,
		(byte)255, (byte)255, (byte)255, (byte)255,
		(byte)255, (byte)255, (byte)255, (byte)255,
		(byte)255, (byte)255, (byte)255, (byte)255
	};
	
	// 指定キーで検索処理.
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected LevelIndexIterator _search(LevelIndexIterator ret, Object columnValue) {
		LeveldbIterator lv = ret.itr;
		boolean reverse = ret.reverse;
		JniBuffer keyBuf = null;
		
		try {
			if(reverse) {
				keyBuf = LevelBuffer.key(indexKeyType, columnValue, MAX_BIN);
			} else {
				keyBuf = LevelBuffer.key(indexKeyType, columnValue, MIN_BIN);
			}
			lv.seek(keyBuf);
			if(lv.valid()) {
				if(reverse) {
					// 逆カーソル移動の場合は、対象keyより大きな値の条件の手前まで移動.
					Comparable c;
					TwoKey tw;
					c = (Comparable)LevelId.id(indexKeyType, columnValue, MAX_BIN);
					while(lv.valid()) {
						keyBuf.position(0);
						lv.key(keyBuf);
						tw = (TwoKey)LevelId.get(indexKeyType, keyBuf);
						if(c.compareTo(tw.get(0)) < 0) {
							lv.before();
							break;
						}
						lv.next();
					}
					if(!lv.valid()) {
						lv.last();
					}
				} else {
					// 通常カーソル移動の場合は、対象keyより小さな値の条件の最初の位置まで移動.
					Comparable c;
					TwoKey tw;
					c = (Comparable)LevelId.id(indexKeyType, columnValue, MIN_BIN);
					while(lv.valid()) {
						keyBuf.position(0);
						lv.key(keyBuf);
						tw = (TwoKey)LevelId.get(indexKeyType, keyBuf);
						if(c.compareTo(tw.get(0)) > 0) {
							lv.next();
							break;
						}
						lv.before();
					}
					if(!lv.valid()) {
						lv.first();
					}
				}
			}
			return ret;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, null);
		}
	}
	
	/**
	 * インデックス情報を生成.
	 * @return long インデックス化された件数が返却されます.
	 */
	public long toIndex() {
		return toIndex(null);
	}
	
	/**
	 * インデックス情報を生成.
	 * @param outError エラー内容を格納します.
	 * @return long インデックス化された件数が返却されます.
	 */
	public long toIndex(List<Exception> outError) {
		checkClose();
		// インデックスを作成.
		LeveldbIterator it = null;
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			long ret = 0L;
			Object value;
			byte[] keyBin;
			
			// ロールバック処理.
			super.rollback();
			// 全データを削除.
			super.trancate();
			
			it = parent.snapshot();
			keyBuf = LevelBuffer.key();
			valBuf = LevelBuffer.value();
			while(it.valid()) {
				try {
					it.key(keyBuf);
					it.value(valBuf);
					it.next();
					value = LevelValues.decode(valBuf);
					valBuf.position(0);
					// インデックス元のvalueがMapじゃない場合、カラムが存在しない場合は処理しない.
					if(!(value instanceof Map) ||
						(value = getValueInColumns(indexColumnList, value)) == null ||
						(value = convertColumType(indexColumnType, value)) == null) {
						continue;
					}
					// キーには、インデックスvalueとインデックス元のキー情報を設定.
					keyBin = keyBuf.getBinary();
					keyBuf.position(0);
					keyBuf = LevelBuffer.key(indexKeyType, value, keyBin);
					// value に インデックス元のキー情報を設定.
					valBuf.setBinary(keyBin);
					keyBin = null;
					leveldb.put(keyBuf, valBuf);
					ret ++;
				} catch (Exception e) {
					if(outError != null && outError.size() < MAX_ERROR) {
						outError.add(e);
					}
				} finally {
					keyBuf.position(0);
					valBuf.position(0);
				}
			}
			it.close();
			it = null;
			return ret;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			if(it != null) {
				it.close();
			}
			LevelBuffer.clearBuffer(keyBuf, valBuf);
		}
	}
	
	/**
	 * Levelインデックスイテレータ.
	 */
	@SuppressWarnings({ "rawtypes" })
	public static final class LevelIndexIterator extends LevelIterator<Object, Map> {
		LevelIndex base; // インデックスオブジェクト.
		Leveldb parent; // インデックス元のleveldb.
		Leveldb index; // インデックスのleveldb.
		LeveldbIterator itr; // インデックスのイテレータ.
		
		LevelIndexIterator(boolean r, LevelIndex o, LeveldbIterator i) {
			base = o;
			parent = o.parent;
			index = o.leveldb;
			reverse = r;
			itr = i;
		}

		// ファイナライズ.
//		protected void finalize() throws Exception {
//			close();
//		}

		@Override
		public void close() {
			if (itr != null) {
				itr.close();
				itr = null;
			}
		}
		
		@Override
		public boolean isClose() {
			return itr == null || itr.isClose();
		}

		@Override
		public boolean isReverse() {
			return reverse;
		}

		@Override
		public boolean hasNext() {
			if (base.isClose() || itr == null || !itr.valid()) {
				close();
				return false;
			}
			return true;
		}
		
		@Override
		public Map next() {
			if (base.isClose() || itr == null || !itr.valid()) {
				close();
				throw new NoSuchElementException();
			}
			Object ret;
			JniBuffer keyBuf = null;
			JniBuffer valBuf = null;
			try {
				// mapの情報で、インデックスを含むものだけを取得.
				while(true) {
					if(keyBuf == null) {
						keyBuf = LevelBuffer.key();
					} else {
						keyBuf.position(0);
					}
					itr.value(keyBuf);
					if(reverse) {
						itr.before();
					} else {
						itr.next();
						if(valBuf == null) {
							valBuf = LevelBuffer.value();
						} else {
							valBuf.position(0);
						}
						itr.value(valBuf);
						valBuf.position(0);
					}
					if (!itr.valid()) {
						close();
					}
					if(valBuf == null) {
						valBuf = LevelBuffer.value();
					} else {
						valBuf.position(0);
					}
					if(parent.get(valBuf, keyBuf) == 0) {
						continue;
					}
					ret = LevelValues.decode(valBuf);
					// インデックスの条件と違うものは取得しない.
					if(convertColumType(base.indexColumnType,
						LevelIndex.getValueInColumns(base.indexColumnList, ret)) != null) {
						this.resultKey = LevelId.get(parent.getType(), keyBuf);
						return (Map)ret;
					}
				}
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			} finally {
				LevelBuffer.clearBuffer(keyBuf, valBuf);
			}
		}
	}
}
