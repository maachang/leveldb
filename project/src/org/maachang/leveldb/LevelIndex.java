package org.maachang.leveldb;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Levelインデックス.
 */
public class LevelIndex extends CommitRollback {
	protected static final int MAX_ERROR = 32;
	
	protected Leveldb parent;
	protected int parentType;
	protected int columnType;
	protected String[] columnNames;
	
	/**
	 * コンストラクタ.
	 * Writebatch無効で作成.
	 * カラム名は hoge.moge.abc のように [.]区切りにすることで、階層で設定できます.
	 * 
	 * @param columnType
	 * @param columnName
	 * @param parent
	 */
	public LevelIndex(int columnType, String columnName, Leveldb parent) {
		if(LevelOption.TYPE_PARAM_LENGTH[columnType] != 1) {
			throw new LeveldbException("Set the column type as a single type " +
				"(LevelOption string or number32 or number64 or binary).");
		}
		String columnString = LevelOption.stringType(columnType);
		int indexKeyType = LevelOption.convertType(columnString + "-" + "binary");
		LevelOption pOpt = parent.option;
		LevelOption opt = LevelOption.create(
			indexKeyType,
			pOpt.getWriteBufferSize(),
			pOpt.getMaxOpenFiles(),
			pOpt.getBlockSize(),
			pOpt.getBlockCache());
		Leveldb db = new Leveldb(parent.getPath() + "@" + columnName, opt);
		
		// leveldbをクローズしてwriteBatchで処理しない.
		super.init(db, true, false);
		this.parent = parent;
		this.parentType = pOpt.type;
		this.columnType = columnType;
		this.columnNames = columnName.split("\\.");
	}
	
	/**
	 * コンストラクタ.
	 * WriteBatchを有効にして作成.
	 * 
	 * @param parnet
	 */
	public LevelIndex(LevelIndex parent) {
		this.parent = parent.leveldb;
		this.parentType = parent.parentType;
		this.columnType = parent.columnType;
		this.columnNames = parent.columnNames;
		
		// leveldbをクローズせずwriteBatchで処理する.
		super.init(parent.leveldb, false, true);
	}
	
	/**
	 * インデックス対象のカラムタイプを取得.
	 * @return
	 */
	public int getColumnType() {
		checkClose();
		return columnType;
	}
	
	/**
	 * インデックス対象のカラム名を取得.
	 * @return
	 */
	public String getColumnName() {
		checkClose();
		StringBuilder buf = new StringBuilder();
		int len = columnNames.length;
		for(int i = 0; i < len; i ++) {
			if(i != 0) {
				buf.append(".");
			}
			buf.append(columnNames[i]);
		}
		return buf.toString();
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
	
	// キーバイナリを取得.
	private static final byte[] keyBinary(int type, Object key1, Object key2) {
		JniBuffer keyBuf = null;
		try {
			keyBuf = LevelBuffer.key(type, key1, key2);
			byte[] ret = keyBuf.getBinary();
			LevelBuffer.clearBuffer(keyBuf, null);
			keyBuf = null;
			return ret;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, null);
		}
	}
	
	// カラム情報に対するValue情報を取得.
	@SuppressWarnings("rawtypes")
	private static final Object getColumnByValue(String[] columnNames, Object value) {
		// valueがMapじゃない場合はインデックス化しない.
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
		Object o = getColumnByValue(columnNames, value);
		if(o == null) {
			return false;
		}
		return putIndexColumn(key, twoKey, o);
	}
	
	/**
	 * インデックスカラムを直接指定して、追加.
	 * @param key
	 * @param twoKey
	 * @param value
	 * @return
	 */
	public boolean putIndexColumn(Object key, Object twoKey, Object value) {
		checkClose();
		// インデックスカラムがnullの場合はインデックス化しない.
		if(value == null) {
			return false;
		}
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			// keyをバイナリ変換して、indexKeyとして[column, binary]の２キーをキーとする.
			// valueには、keyをバイナリ変換したものをセットする.
			byte[] keyBin = keyBinary(parentType, key, twoKey);
			keyBuf = LevelBuffer.key(columnType, value, keyBin);
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
		Object o = getColumnByValue(columnNames, value);
		if(o == null) {
			return false;
		}
		return removeIndexColumn(key, twoKey, o);
	}
	
	/**
	 * インデックスカラムを直接指定して、削除.
	 * @param key
	 * @param twoKey
	 * @param value
	 * @return
	 */
	public boolean removeIndexColumn(Object key, Object twoKey, Object value) {
		checkClose();
		if(value == null) {
			return false;
		}
		JniBuffer keyBuf = null;
		try {
			// keyをバイナリ変換して、indexKeyとして[column, binary]の２キーをキーとする.
			byte[] keyBin = keyBinary(parentType, key, twoKey);
			keyBuf = LevelBuffer.key(columnType, value, keyBin);
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
	 * Levelインデックスイテレータ.
	 */
	public static final class LevelIndexIterator
		implements LevelIterator<Map<String,Object>> {
		LevelIndex base;
		Leveldb src;
		Leveldb index;
		LeveldbIterator itr;
		boolean reverse;
		
		LevelIndexIterator(boolean r, LevelIndex o, LeveldbIterator i) {
			base = o;
			src = o.parent;
			index = o.leveldb;
			reverse = r;
			itr = i;
		}

		// ファイナライズ.
		protected void finalize() throws Exception {
			close();
		}

		@Override
		public void close() {
			if (itr != null) {
				itr.close();
				itr = null;
			}
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
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public Map<String,Object> next() {
			if (base.isClose() || itr == null || !itr.valid()) {
				close();
				throw new NoSuchElementException();
			}
			JniBuffer keyBuf = null;
			JniBuffer valBuf = null;
			try {
				keyBuf = LevelBuffer.key();
				itr.value(keyBuf);
				if(reverse) {
					itr.before();
				} else {
					itr.next();
				}
				if (!itr.valid()) {
					close();
				}
				valBuf = LevelBuffer.value();
				if(src.get(valBuf, keyBuf) == 0) {
					return null;
				}
				Object ret = LevelValues.decode(valBuf);
				if(ret instanceof Map) {
					return (Map)ret;
				}
				return null;
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			} finally {
				LevelBuffer.clearBuffer(keyBuf, valBuf);
			}
		}
	}
	
	/**
	 * 情報取得.
	 * @param value
	 * @return
	 */
	public LevelIndexIterator get(Object value) {
		checkClose();
		// valueがMapじゃない場合はインデックス化しない.
		Object o = getColumnByValue(columnNames, value);
		if(o == null) {
			return null;
		}
		return _iterator(false, o);
	}
	
	/**
	 * 情報取得.
	 * @param reverse
	 * @param value
	 * @return
	 */
	public LevelIndexIterator get(boolean reverse, Object value) {
		checkClose();
		// valueがMapじゃない場合はインデックス化しない.
		Object o = getColumnByValue(columnNames, value);
		if(o == null) {
			return null;
		}
		return _iterator(reverse, o);
	}
	
	/**
	 * インデックスカラムを直接指定して、情報取得.
	 * @param value
	 * @return
	 */
	public LevelIndexIterator getIndexColumn(Object value) {
		return _iterator(false, value);
	}
	
	/**
	 * インデックスカラムを直接指定して、情報取得.
	 * @param reverse
	 * @param value
	 * @return
	 */
	public LevelIndexIterator getIndexColumn(boolean reverse, Object value) {
		return _iterator(reverse, value);
	}
	
	// イテレータを取得.
	protected LevelIndexIterator _iterator(boolean reverse, Object columnValue) {
		checkClose();
		LevelIndexIterator ret = null;
		try {
			ret = new LevelIndexIterator(reverse, this, leveldb.snapshot());
			return _search(ret, columnValue);
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
	private static final byte[] ZERO_BIN = new byte[0];
	
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
			keyBuf = LevelBuffer.key(columnType, columnValue, ZERO_BIN);
			lv.seek(keyBuf);
			LevelBuffer.clearBuffer(keyBuf, null);
			if(lv.valid() && reverse) {
				// 逆カーソル移動の場合は、対象keyより大きな値の条件の手前まで移動.
				Comparable c, cc;
				c = (Comparable)LevelId.id(columnType, columnValue, MAX_BIN);
				while(lv.valid()) {
					lv.key(keyBuf);
					cc = (Comparable)LevelId.get(columnType, keyBuf);
					LevelBuffer.clearBuffer(keyBuf, null);
					if(c.compareTo(cc) < 0) {
						lv.before();
						break;
					}
					lv.next();
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
			super.clearLeveldb();
			
			it = parent.snapshot();
			keyBuf = LevelBuffer.key();
			valBuf = LevelBuffer.value();
			while(it.valid()) {
				try {
					it.key(keyBuf);
					it.value(valBuf);
					it.next();
					value = LevelValues.decode(valBuf);
					// インデックス元のvalueがMapじゃない場合、カラムが存在しない場合は処理しない.
					if(!(value instanceof Map) ||
						(value = getColumnByValue(columnNames, value)) == null) {
						continue;
					}
					keyBin = keyBuf.getBinary();
					keyBuf = LevelBuffer.key(columnType, value, keyBin);
					valBuf.setBinary(keyBin);
					leveldb.put(keyBuf, valBuf);
					ret ++;
				} catch (Exception e) {
					if(outError != null && outError.size() < MAX_ERROR) {
						outError.add(e);
					}
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
}
