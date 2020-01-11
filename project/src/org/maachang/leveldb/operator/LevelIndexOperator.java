package org.maachang.leveldb.operator;

import java.io.File;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.Leveldb;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.operator.LevelIndex.LevelIndexIterator;
import org.maachang.leveldb.util.Converter;
import org.maachang.leveldb.util.FileUtil;
import org.maachang.leveldb.util.FixedSearchArray;
import org.maachang.leveldb.util.OList;

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
				if(path.startsWith(fname = dirPath + flist[i]) &&
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
				columnType = fname.substring(p + 1, fname.length() - LevelIndex.INDEX_CUT.length());
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
		boolean cflg = closeFlag.get();
		super.close();
		if(cflg) {
			indexLock.writeLock().lock();
			try {
				OList<LevelIndex> lst = indexList;
				indexList = null;
				indexColumns = null;
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
	}
	
	@Override
	public void commit() {
		super.commit();
		if(writeBatchFlag) {
			indexLock.readLock().lock();
			try {
				LevelIndex idx;
				final int len = indexList == null ? 0 : indexList.size();
				for(int i = 0; i < len; i ++) {
					if((idx = indexList.get(i)) != null && !idx.isClose()) {
						idx.commit();
					}
				}
			} finally {
				indexLock.writeLock().unlock();
			}
		}
	}
	
	@Override
	public void rollback() {
		super.rollback();
		if(writeBatchFlag) {
			indexLock.readLock().lock();
			try {
				LevelIndex idx;
				final int len = indexList == null ? 0 : indexList.size();
				for(int i = 0; i < len; i ++) {
					if((idx = indexList.get(i)) != null && !idx.isClose()) {
						idx.rollback();
					}
				}
			} finally {
				indexLock.writeLock().unlock();
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
}
