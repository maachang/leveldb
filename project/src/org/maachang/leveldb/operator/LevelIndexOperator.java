package org.maachang.leveldb.operator;

import java.io.File;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.LevelBuffer;
import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.LevelValues;
import org.maachang.leveldb.Leveldb;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.operator.LevelIndex.LevelIndexIterator;
import org.maachang.leveldb.util.Converter;
import org.maachang.leveldb.util.FileUtil;
import org.maachang.leveldb.util.FixedSearchArray;
import org.maachang.leveldb.util.Json;
import org.maachang.leveldb.util.OList;
import org.maachang.leveldb.util.ObjectList;

/**
 * インデックスがサポートされたオペレータ.
 */
public abstract class LevelIndexOperator extends LevelOperator {
	// インデックス名管理.
	protected FixedSearchArray<String> indexColumns;
	protected OList<LevelIndex> indexList;
	
	// インデックスロック.
	protected ReadWriteLock indexLock;
	
	// 索引用カラムリストの作成.
	private static final FixedSearchArray<String> createSearchArray(OList<LevelIndex> list) {
		int len = list.size();
		FixedSearchArray<String> ret = new FixedSearchArray<String>(len);
		for(int i = 0; i < len; i ++) {
			ret.add(list.get(i).getColumnName(), i);
		}
		return ret;
	}
	
	/**
	 * インデックス情報のための初期処理.
	 * @param src writeBatch対応の場合は、元となる条件を設定する必要があります。
	 */
	protected void initIndex(LevelIndexOperator src) {
		// writeBatchFlag じゃなくて、srcがNULLの場合は、新規ロード.
		if(!writeBatchFlag || src == null) {
			indexLock = new ReentrantReadWriteLock();
			loadIndex();
		} else {
			// コミット・ロールバック用のデータを作成.
			indexLock = src.indexLock;
			indexLock.writeLock().lock();
			try {
				final OList<LevelIndex> srcList = src.indexList;
				final int len = srcList == null ? 0 : srcList.size();
				if(len == 0) {
					// インデックスが無い場合は作成しない.
					return;
				}
				LevelIndex idx;
				final OList<LevelIndex> list = new OList<LevelIndex>(len);
				for(int i = 0; i < len; i ++) {
					if((idx = srcList.get(i)) != null && !idx.isClose()) {
						list.add(new LevelIndex(idx));
					}
				}
				indexColumns = createSearchArray(list);
				indexList = list;
			} finally {
				indexLock.writeLock().unlock();
			}
		}
	}
	
	/**
	 * インデックス情報を読み込み.
	 */
	protected void loadIndex() {
		indexLock.writeLock().lock();
		try {
			// 配下のこのオペレータのインデックス情報を検索.
			String fname;
			String path = FileUtil.getFullPath(leveldb.getPath());
			int p = path.lastIndexOf("/");
			String dirPath = path.substring(0, p + 1);
			File f = new File(dirPath);
			String[] flist = f.list(); f = null;
			int len = flist.length;
			OList<String> list = new OList<String>();
			for(int i = 0; i < len; i ++) {
				fname = dirPath + flist[i];
				if(fname.startsWith(path) &&
					flist[i].endsWith(LevelIndex.INDEX_FOODER) &&
					flist[i].lastIndexOf(LevelIndex.INDEX_CUT) != -1 &&
					FileUtil.isDir(fname)) {
					list.add(flist[i]);
				}
			}
			if((len = list.size()) <= 0) {
				return;
			}
			fname = null;
			dirPath = null;
			path = null;
			flist = null;
			
			// インデックス名から、インデックスを作成して登録.
			int pp;
			String columnName, columnType;
			OList<LevelIndex> idxList = new OList<LevelIndex>(len);
			for(int i = 0; i < len; i ++) {
				fname = list.get(i);
				p = fname.lastIndexOf(LevelIndex.INDEX_CUT);
				pp = fname.lastIndexOf(LevelIndex.INDEX_CUT, p - 1);
				if(pp == -1) {
					continue;
				}
				columnName = fname.substring(pp + 1, p);
				columnType = fname.substring(p + 1, fname.length() - LevelIndex.INDEX_FOODER.length());
				if(!Converter.isNumeric(columnType)) {
					continue;
				}
				idxList.add(new LevelIndex(Converter.convertInt(columnType), columnName, leveldb));
			}
			if((len = idxList.size()) <= 0) {
				return;
			}
			indexColumns = createSearchArray(idxList);
			indexList = idxList;
		} catch(LeveldbException le) {
			throw le;
		} catch(Exception e) {
			throw new LeveldbException(e);
		} finally {
			indexLock.writeLock().unlock();
		}
	}
	
	// インデックスにデータを追加.
	protected void putIndex(Object key, Object twoKey, Object value) {
		indexLock.readLock().lock();
		try {
			// 現在の全インデックスにデータ登録.
			LevelIndex idx;
			final int len = indexList == null ? 0 : indexList.size();
			if(len > 0 && value instanceof JniBuffer) {
				JniBuffer buf = null;
				try {
					buf = (JniBuffer) value;
					value = LevelValues.decode(buf);
				} catch(Exception e) {
					throw new LeveldbException(e);
				} finally {
					LevelBuffer.clearBuffer(null, buf);
				}
			}
			for(int i = 0; i < len; i ++) {
				if((idx = indexList.get(i)) != null && !idx.isClose()) {
					idx.put(key, twoKey, value);
				}
			}
		} finally {
			indexLock.readLock().unlock();
		}
	}
	
	// インデックスにデータを削除.
	protected void removeIndex(Object key, Object twoKey, Object value) {
		indexLock.readLock().lock();
		try {
			// 現在の全インデックスにデータ削除.
			LevelIndex idx;
			final int len = indexList == null ? 0 : indexList.size();
			if(len > 0 && value instanceof JniBuffer) {
				JniBuffer buf = null;
				try {
					buf = (JniBuffer) value;
					value = LevelValues.decode(buf);
				} catch(Exception e) {
					throw new LeveldbException(e);
				} finally {
					LevelBuffer.clearBuffer(null, buf);
				}
			}
			for(int i = 0; i < len; i ++) {
				if((idx = indexList.get(i)) != null && !idx.isClose()) {
					idx.remove(key, twoKey, value);
				}
			}
		} finally {
			indexLock.readLock().unlock();
		}
	}
	
	@Override
	public void close() {
		if(closeFlag.get()) {
			closeIndex();
			super.close();
		}
	}
	
	// インデックス情報のクローズ.
	private void closeIndex() {
		indexLock.writeLock().lock();
		try {
			OList<LevelIndex> lst = indexList;
			indexList = null; indexColumns = null;
			final int len = lst == null ? 0 : lst.size();
			for(int i = 0; i < len; i ++) {
				try {
					lst.get(i).close();
				} catch(Exception e) {}
			}
		} finally {
			indexLock.writeLock().unlock();
		}
	}
	
	// このオペレータを完全破棄.
	@Override
	public boolean deleteComplete() {
		if(super.deleteComplete()) {
			List<Exception> errs = new ObjectList<Exception>();
			deleteAllIndexComplete(errs);
			// エラーの場合は最初のエラーを返却.
			if(errs.size() > 0) {
				if(errs.get(0) instanceof LeveldbException) {
					throw (LeveldbException)errs.get(0);
				}
				throw new LeveldbException(errs.get(0));
			}
			return true;
		}
		return false;
	}
	
	// 全インデックス情報の完全削除処理.
	// true返却でエラー.
	private boolean deleteAllIndexComplete(List<Exception> errs) {
		indexLock.writeLock().lock();
		try {
			boolean resultError = false;
			OList<LevelIndex> list = indexList;
			if(list != null) {
				indexList = null; indexColumns = null;
				final int len = list.size();
				// 全削除.
				for(int i = 0; i < len; i ++) {
					try {
						list.get(i).deleteComplete();
					} catch(Exception e) {
						if(errs != null) {
							errs.add(e);
						}
						resultError = true;
					}
				}
				list.clear();
			}
			return resultError;
		} finally {
			indexLock.writeLock().unlock();
		}
	}
	
	@Override
	public boolean trancate() {
		if(super.trancate()) {
			List<Exception> errs = new ObjectList<Exception>();
			trancateAllIndex(errs);
			// エラーの場合は最初のエラーを返却.
			if(errs.size() > 0) {
				if(errs.get(0) instanceof LeveldbException) {
					throw (LeveldbException)errs.get(0);
				}
				throw new LeveldbException(errs.get(0));
			}
			return true;
		}
		return false;
	}
	
	// 全インデックス情報のデータ削除.
	// true返却でエラー.
	private boolean trancateAllIndex(List<Exception> errs) {
		indexLock.writeLock().lock();
		try {
			boolean resultError = false;
			OList<LevelIndex> list = indexList;
			if(list != null) {
				LevelIndex idx;
				final int len = list.size();
				// データ削除.
				for(int i = 0; i < len; i ++) {
					idx = list.get(i);
					try {
						idx.trancate();
						// 現在格納されているインデックス親オブジェクトは、
						// 一旦クローズされているので、再セットする.
						idx.parent = leveldb;
					} catch(Exception e) {
						if(errs != null) {
							errs.add(e);
						}
						resultError = true;
					}
				}
			}
			return resultError;
		} finally {
			indexLock.writeLock().unlock();
		}
	}
	
	@Override
	public void commit() {
		super.commit();
		if(writeBatchFlag) {
			Exception err = null;
			indexLock.readLock().lock();
			try {
				LevelIndex idx;
				final int len = indexList == null ? 0 : indexList.size();
				for(int i = 0; i < len; i ++) {
					if((idx = indexList.get(i)) != null && !idx.isClose()) {
						try {
							idx.commit();
						} catch(Exception e) {
							err = e;
						}
					}
				}
			} finally {
				indexLock.readLock().unlock();
			}
			if(err != null) {
				if(err instanceof LeveldbException) {
					throw (LeveldbException)err;
				}
				throw new LeveldbException(err);
			}
		}
	}
	
	@Override
	public void rollback() {
		super.rollback();
		if(writeBatchFlag) {
			Exception err = null;
			indexLock.readLock().lock();
			try {
				LevelIndex idx;
				final int len = indexList == null ? 0 : indexList.size();
				for(int i = 0; i < len; i ++) {
					if((idx = indexList.get(i)) != null && !idx.isClose()) {
						try {
							idx.rollback();
						} catch(Exception e) {
							err = e;
						}
					}
				}
			} finally {
				indexLock.readLock().unlock();
			}
			if(err != null) {
				if(err instanceof LeveldbException) {
					throw (LeveldbException)err;
				}
				throw new LeveldbException(err);
			}
		}
	}
	
	/**
	 * 新しいインデックスを作成.
	 * 
	 * @param columnType インデックスカラムタイプ.
	 * @param column インデクスカラム名を設定します.
	 *               設定方法は、hoge.moge.abc や "hoge", "moge", "abc"のように階層設定可能.
	 */
	public void createIndex(int columnType, String... column) {
		checkClose();
		// writeBatchモードの場合は、この処理は実行出来ない.
		if(writeBatchFlag) {
			throw new LeveldbException("This process cannot be used in writeBatch mode.");
		} else if(column == null || column.length == 0) {
			throw new NullPointerException();
		}
		final String columnName = LevelIndex.srcColumnNames(column);
		indexLock.writeLock().lock();
		try {
			// 既に登録されているカラム名の場合.
			if(indexColumns != null && indexColumns.search(columnName) != -1) {
				throw new LeveldbException("Index of column name '"+ columnName + "' is already registered.");
			}
			final LevelIndex idx = new LevelIndex(columnType, columnName, leveldb);
			// 現在のデータ分インデックスを作成する.
			idx.toIndex(null);
			// インデックス情報の登録.
			if(indexList != null) {
				indexList.add(idx);
				indexColumns.add(columnName, indexList.size() - 1);
			} else {
				indexList = new OList<LevelIndex>();
				indexList.add(idx);
				indexColumns = new FixedSearchArray<String>(columnName);
			}
		} finally {
			indexLock.writeLock().unlock();
		}
	}
	
	/**
	 * インデックスを削除.
	 * 
	 * @param column インデクスカラム名を設定します.
	 *               設定方法は、hoge.moge.abc や "hoge", "moge", "abc"のように階層設定可能.
	 */
	public void deleteIndex(String... column) {
		checkClose();
		// writeBatchモードの場合は、この処理は実行出来ない.
		if(writeBatchFlag) {
			throw new LeveldbException("This process cannot be used in writeBatch mode.");
		} else if(column == null || column.length == 0) {
			throw new NullPointerException();
		}
		final String columnName = LevelIndex.srcColumnNames(column);
		indexLock.writeLock().lock();
		try {
			// 登録されていないカラム名の場合.
			final int no = indexColumns == null ? -1 : indexColumns.search(columnName);
			final LevelIndex idx = no == -1 ? null : indexList.get(no);
			if(idx == null || idx.isClose()) {
				throw new LeveldbException("Index of column name '"+ columnName + "' does not exist.");
			}
			// インデックスの物理削除.
			String path = idx.getPath();
			LevelOption opt = idx.getOption();
			idx.close();
			Leveldb.destroy(path, opt);
			// 管理領域から削除.
			if(indexList.size() == 1) {
				indexList = null;
				indexColumns = null;
			} else {
				// FixedSearchArrayを作成しなおす.
				indexList.remove(no);
				indexColumns = createSearchArray(indexList);
			}
		} finally {
			indexLock.writeLock().unlock();
		}
	}
	
	/**
	 * インデックス情報を取得.
	 * 昇順で情報を取得します.
	 * 
	 * @param value 検索対象の要素を設定します.
	 * @param column インデクスカラム名を設定します.
	 *               設定方法は、hoge.moge.abc や "hoge", "moge", "abc"のように階層設定可能.
	 * @return
	 */
	public LevelIndexIterator getIndexAsc(Object value, String... column) {
		return getIndex(false, value, column);
	}
	
	/**
	 * インデックス情報を取得.
	 * 降順で情報を取得します.
	 * 
	 * @param value 検索対象の要素を設定します.
	 * @param column インデクスカラム名を設定します.
	 *               設定方法は、hoge.moge.abc や "hoge", "moge", "abc"のように階層設定可能.
	 * @return
	 */
	public LevelIndexIterator getIndexDesc(Object value, String... column) {
		return getIndex(true, value, column);
	}
	
	/**
	 * インデックス情報を取得.
	 * 
	 * @param desc [true]の場合、降順で情報を取得します.
	 * @param value 検索対象の要素を設定します.
	 * @param column インデクスカラム名を設定します.
	 *               設定方法は、hoge.moge.abc や "hoge", "moge", "abc"のように階層設定可能.
	 * @return
	 */
	public LevelIndexIterator getIndex(boolean desc, Object value, String... column) {
		checkClose();
		if(column == null || column.length == 0) {
			throw new NullPointerException();
		}
		final String idxColumn = LevelIndex.srcColumnNames(column);
		indexLock.readLock().lock();
		try {
			final int no = indexColumns == null ? -1 : indexColumns.search(idxColumn);
			final LevelIndex idx = no == -1 ? null : indexList.get(no);
			if(idx == null || idx.isClose()) {
				throw new LeveldbException("Index information of column name '"+
					idxColumn + "' does not exist.");
			}
			if(value == null) {
				return idx.getSortIndex(desc);
			} else {
				return idx.get(desc, value);
			}
		} finally {
			indexLock.readLock().unlock();
		}
	}
	
	/**
	 * LevelIndexオブジェクトを取得.
	 * 
	 * @param column インデクスカラム名を設定します.
	 *               設定方法は、hoge.moge.abc や "hoge", "moge", "abc"のように階層設定可能.
	 * @return
	 */
	public LevelIndex getLevelIndex(String... column) {
		checkClose();
		if(column == null || column.length == 0) {
			throw new NullPointerException();
		}
		final String idxColumn = LevelIndex.srcColumnNames(column);
		indexLock.readLock().lock();
		try {
			final int no = indexColumns == null ? -1 : indexColumns.search(idxColumn);
			final LevelIndex idx = no == -1 ? null : indexList.get(no);
			if(idx == null || idx.isClose()) {
				throw new LeveldbException("Index information of column name '"+
					idxColumn + "' does not exist.");
			}
			return idx;
		} finally {
			indexLock.readLock().unlock();
		}
	}
	
	/**
	 * インデックスが存在するかチェック.
	 * 
	 * @param column インデクスカラム名を設定します.
	 *               設定方法は、hoge.moge.abc や "hoge", "moge", "abc"のように階層設定可能.
	 * @return boolean [true]の場合、存在します.
	 */
	public boolean isIndex(String... column) {
		checkClose();
		if(column == null || column.length == 0) {
			throw new NullPointerException();
		}
		final String idxColumn = LevelIndex.srcColumnNames(column);
		indexLock.readLock().lock();
		try {
			final int no = indexColumns == null ? -1 : indexColumns.search(idxColumn);
			if(no == -1) {
				return false;
			}
			return !indexList.get(no).isClose();
		} finally {
			indexLock.readLock().unlock();
		}
	}
	
	/**
	 * インデックス数を取得.
	 * 
	 * @return int インデックス数を取得します.
	 */
	public int indexSize() {
		checkClose();
		indexLock.readLock().lock();
		try {
			int ret = 0;
			LevelIndex idx;
			final int len = indexList == null ? 0 : indexList.size();
			for(int i = 0; i < len; i ++) {
				if((idx = indexList.get(i)) != null && !idx.isClose()) {
					ret ++;
				}
			}
			return ret;
		} finally {
			indexLock.readLock().unlock();
		}
	}
	
	/**
	 * インデックスカラム名群を取得.
	 * 
	 * @return String[] インデックスカラム名群が返却されます.
	 */
	public String[] indexColumns() {
		checkClose();
		indexLock.readLock().lock();
		try {
			int len = indexList == null ? 0 : indexList.size();
			if(len == 0) {
				return new String[len];
			}
			LevelIndex idx;
			OList<String> lst = new OList<String>();
			for(int i = 0; i < len; i ++) {
				if((idx = indexList.get(i)) != null && !idx.isClose()) {
					lst.add(idx.getColumnName());
				}
			}
			len = lst.size();
			String[] ret = new String[len];
			System.arraycopy(lst.toArray(), 0, ret, 0, len);
			return ret;
		} finally {
			indexLock.readLock().unlock();
		}
	}
	
	/**
	 * インデックスタイプとインデックスカラム名を取得.
	 * @param column インデックスカラム名を設定します.
	 * @return Object[] [0]インデックスタイプ, [1]インデックスカラム名が返却されます.
	 */
	public Object[] getIndexConfig(String... column) {
		checkClose();
		if(column == null || column.length == 0) {
			throw new NullPointerException();
		}
		final String idxColumn = LevelIndex.srcColumnNames(column);
		indexLock.readLock().lock();
		try {
			final int no = indexColumns == null ? -1 : indexColumns.search(idxColumn);
			if(no == -1) {
				return null;
			}
			LevelIndex idx = indexList.get(no);
			return new Object[] {
				idx.getColumnType(),
				idx.getColumnName()
			};
		} finally {
			indexLock.readLock().unlock();
		}
	}
	
	// インデックスが存在するかチェ)ック.
	protected boolean indexEmpty() {
		checkClose();
		indexLock.readLock().lock();
		try {
			return indexList == null || indexList.size() <= 0;
		} finally {
			indexLock.readLock().unlock();
		}
	}
	
	// インデックス生成群の条件を取得.
	protected Object[] indexInfo() {
		checkClose();
		indexLock.readLock().lock();
		try {
			LevelIndex idx;
			int len = indexList == null ? 0 : indexList.size();
			Object[] ret = new Object[len];
			for(int i = 0; i < len; i ++) {
				idx = indexList.get(i);
				ret[i] = new Object[] {
					idx.indexColumnName,			// インデックスカラム名.
					idx.indexColumnType,			// インデックスカラムタイプ.
					idx.getOption().copyObject()	// 生成オプション.
				};
			}
			return ret;
		} finally {
			indexLock.readLock().unlock();
		}
	}
}
