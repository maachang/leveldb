package org.maachang.leveldb.operator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.LevelBuffer;
import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.Leveldb;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.Time12SequenceId;
import org.maachang.leveldb.operator.LevelMap.LevelMapIterator;
import org.maachang.leveldb.util.FileUtil;
import org.maachang.leveldb.util.Flag;
import org.maachang.leveldb.util.ObjectList;

/**
 * LevelOperatorマネージャ.
 */
public class LevelOperatorManager {
	
	// マネージャパス.
	protected static final String MANAGER_PATH = LevelOperatorConstants.MANAGER_FOLDER;

	// オペレータパス.
	protected static final String OPERATOR_PATH = LevelOperatorConstants.OPERATOR_FOLDER;

	// ユニークオブジェクト名のキー名の先頭名.
	private static final String _UNIQUE_HEADER = "@u_";

	// オブジェクト名とユニークオブジェクト名の先頭名.
	private static final String _NAME_HEADER = "@n_";
	
	// 緯度経度管理オブジェクト名.
	private static final String LATLON_NAME = "@ll@";
	
	// シーケンスIDオブジェクト名.
	private static final String SEQUENCE_NAME = "@sq@";
	
	// キューオブジェクト名.
	private static final String QUEUE_NAME = "@qu@";
	
	// MAPオブジェクト名.
	private static final String MAP_NAME = "@mp@";
	
	// ベースパス.
	private String basePath = "./";
	
	// マシンID.
	private int machineId = 0;

	// マシンIDやテーブル名を管理する leveldb.
	private LevelMap manager = null;
	
	// ユニーク名を作成するTime12SequenceId.
	private Time12SequenceId uniqueManager = null;

	// オブジェクト情報管理.
	// key = ユニークオブジェクト名.
	private Map<String, LevelOperator> operatorMemManager = new ConcurrentHashMap<String, LevelOperator>();

	// オブジェクト名管理.
	private Map<String, String> nameMemManager = new ConcurrentHashMap<String, String>();

	// クローズフラグ.
	private final Flag closeFlag = new Flag();

	// rwlock.
	private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

	/**
	 * コンストラクタ.
	 * 
	 * @param machineId
	 */
	public LevelOperatorManager(int machineId) {
		this(LevelOperatorConstants.DEFAULT_LEVEL_DB_FOLDER, machineId);
	}
	
	/**
	 * コンストラクタ.
	 * 
	 * @param path
	 * @param machineId
	 */
	public LevelOperatorManager(String path, int machineId) {
		try {
			path = FileUtil.getFullPath(path);
		} catch(Exception e) {
			throw new LeveldbException(e);
		}
		if(!path.endsWith("/")) {
			path += "/";
		}
		LevelOption opt = LevelOption.create(LevelOption.TYPE_STRING);
		LevelMap map = new LevelMap(path + MANAGER_PATH, opt);

		this.basePath = path;
		this.machineId = machineId;
		this.uniqueManager = new Time12SequenceId(machineId);
		this.manager = map;
		this.closeFlag.set(false);
	}

	// ファイナライズ.
	protected void finalize() throws Exception {
		this.close();
	}

	/**
	 * クローズ処理.
	 */
	public void close() {
		rwLock.writeLock().lock();
		try {
			if (!closeFlag.setToGetBefore(true)) {
				Map<String, LevelOperator> o = operatorMemManager;
				operatorMemManager = null;
				if (o != null) {
					Iterator<Entry<String, LevelOperator>> it = o.entrySet().iterator();
					while (it.hasNext()) {
						it.next().getValue().close();
					}
				}
				nameMemManager = null;
				LevelMap map = manager;
				manager = null;
				if (map != null) {
					map.close();
				}
			}
		} finally {
			rwLock.writeLock().unlock();
		}
	}

	// クローズチェック.
	private final void closeCheck() {
		if (closeFlag.get()) {
			throw new LeveldbException("Already closed.");
		}
	}

	/**
	 * オブジェクトがクローズしているかチェック.
	 * 
	 * @return
	 */
	public boolean isClose() {
		rwLock.readLock().lock();
		try {
			return closeFlag.get();
		} finally {
			rwLock.readLock().unlock();
		}
	}

	/**
	 * マシンIDを取得.
	 * 
	 * @return
	 */
	public int getMachineId() {
		return machineId;
	}

	// 指定オブジェクト名からユニークオブジェクト名を取得.
	protected final String getUniqueName(String name) {
		return (String) manager.get(_NAME_HEADER + name);
	}

	// 指定ユニーク名からオブジェクト名を取得.
	protected final String getObjectName(String uname) {
		String key, value;
		String target = _UNIQUE_HEADER + uname;
		LevelMapIterator it = manager.iterator();
		while(it.hasNext()) {
			if((key = (String)it.next()).startsWith(_NAME_HEADER)) {
				value = (String)manager.get(key);
				if(target.equals(value)) {
					return key.substring(_NAME_HEADER.length());
				}
			}
		}
		return null;
	}

	/**
	 * Mapオブジェクトを生成.
	 * 
	 * @param name
	 *            オブジェクト名を設定します.
	 * @param opt
	 *            オブジェクト用のLeveldbOption を設定します.
	 * @return [true]の場合、生成成功です.
	 */
	public boolean createMap(String name, LevelOption opt) {
		return create(name, LevelOperator.LEVEL_MAP, opt);
	}
	
	/**
	 * 新しい緯度経度用オブジェクトオブジェクトを生成.
	 * 
	 * @param name
	 *            オブジェクト名を設定します.
	 * @param opt
	 *            オブジェクト用のLeveldbOption を設定します.
	 * @return [true]の場合、生成成功です.
	 */
	public boolean createLatLon(String name, LevelOption opt) {
		return create(name, LevelOperator.LEVEL_LAT_LON, opt);
	}
	
	/**
	 * シーケンスオブジェクトオブジェクトを生成.
	 * 
	 * @param name
	 *            オブジェクト名を設定します.
	 * @param opt
	 *            オブジェクト用のLeveldbOption を設定します.
	 * @return [true]の場合、生成成功です.
	 */
	public boolean createSequence(String name, LevelOption opt) {
		return create(name, LevelOperator.LEVEL_SEQUENCE, opt);
	}

	/**
	 * キューオブジェクトオブジェクトを生成.
	 * 
	 * @param name
	 *            オブジェクト名を設定します.
	 * @return [true]の場合、生成成功です.
	 */
	public boolean createQueue(String name) {
		return create(name, LevelOperator.LEVEL_QUEUE, null);
	}

	/**
	 * 新しいオブジェクトを生成.
	 * 
	 * @param name
	 *            オブジェクト名を設定します.
	 * @param objectType
	 *            オブジェクトタイプを設定します.
	 * @param opt
	 *            オブジェクト用のLeveldbOption を設定します.
	 * @return [true]の場合、生成成功です.
	 */
	public boolean create(String name, int objectType, LevelOption opt) {
		JniBuffer valBuf = null;
		rwLock.writeLock().lock();
		try {
			closeCheck();
			// 既に同一名の情報が存在する場合.
			if (manager.containsKey(_NAME_HEADER + name)) {
				return false;
			}
			valBuf = LevelBuffer.value();
			try {
				String uname = Time12SequenceId.toString(uniqueManager.next());
				switch(objectType) {
				case LevelOperator.LEVEL_LAT_LON:
					uname = LATLON_NAME + uname;
					break;
				case LevelOperator.LEVEL_SEQUENCE:
					uname = SEQUENCE_NAME + uname;
					break;
				case LevelOperator.LEVEL_QUEUE:
					uname = QUEUE_NAME + uname;
					break;
				default:
					uname = MAP_NAME + uname;
					break;
				}
				if(opt == null) {
					// タイプなしで作成.
					opt = LevelOption.create(LevelOption.TYPE_NONE);
				}
				opt.toBuffer(valBuf);
				manager.put(_UNIQUE_HEADER + uname, valBuf);
				LevelBuffer.clearBuffer(null, valBuf);
				valBuf = null;
				manager.put(_NAME_HEADER + name, uname);
				return true;
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			}
		} finally {
			rwLock.writeLock().unlock();
			LevelBuffer.clearBuffer(null, valBuf);
		}
	}
	
	/**
	 * オブジェクト情報の完全削除.
	 * 
	 * @param name
	 * @param opt
	 */
	public void destroyOperator(String name, LevelOption opt) {
		Leveldb.destroy(basePath + OPERATOR_PATH + name, opt);
	}

	/**
	 * 指定オブジェクトを削除.
	 * 
	 * @param name
	 * @return
	 */
	public boolean delete(String name) {
		JniBuffer valBuf = null;
		rwLock.writeLock().lock();
		try {
			closeCheck();
			// 対象の情報が存在しない場合.
			if (!manager.containsKey(_NAME_HEADER + name)) {
				return false;
			}
			valBuf = LevelBuffer.value();
			try {
				String uname = getUniqueName(name);
				if (uname == null || !manager.getBuffer(valBuf, _UNIQUE_HEADER + uname)) {
					return false;
				}
				LevelOption opt = new LevelOption(valBuf);
				LevelBuffer.clearBuffer(null, valBuf);
				valBuf = null;
				// オブジェクトがオープンの場合は、強制クローズ.
				LevelOperator obj = operatorMemManager.get(uname);
				if (obj != null) {
					obj.close();
					obj = null;
				}
				destroyOperator(uname, opt);
				manager.remove(_UNIQUE_HEADER + uname);
				manager.remove(_NAME_HEADER + name);
				operatorMemManager.remove(uname);
				nameMemManager.remove(name);
				return true;
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			}
		} finally {
			rwLock.writeLock().unlock();
			LevelBuffer.clearBuffer(null, valBuf);
		}
	}

	/**
	 * 指定オブジェクトの名前変更.
	 * 
	 * @param src
	 * @param dest
	 * @return
	 */
	public boolean rename(String src, String dest) {
		rwLock.writeLock().lock();
		try {
			closeCheck();
			// 対象の情報が存在しない場合.
			if (!manager.containsKey(_NAME_HEADER + src)) {
				return false;
			} else if (manager.containsKey(_NAME_HEADER + dest)) {
				throw new LeveldbException("The operator name for \'" + dest + "\' already exists.");
			}
			try {
				String uname = getUniqueName(src);
				manager.remove(_NAME_HEADER + src);
				manager.put(_NAME_HEADER + dest, uname);
				if(nameMemManager.containsKey(src)) {
					nameMemManager.remove(src);
					nameMemManager.put(dest, uname);
				}
				return true;
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			}
		} finally {
			rwLock.writeLock().unlock();
		}
	}
	
	// オブジェクトを取得.
	private LevelOperator _get(String name) {
		JniBuffer valBuf = null;
		try {
			if (!manager.containsKey(_NAME_HEADER + name)) {
				return null;
			}
			String uname = getUniqueName(name);
			valBuf = LevelBuffer.value();
			if (!manager.getBuffer(valBuf, _UNIQUE_HEADER + uname)) {
				return null;
			}
			LevelOption opt = new LevelOption(valBuf);
			LevelBuffer.clearBuffer(null, valBuf);
			valBuf = null;
			String dbName = basePath + OPERATOR_PATH + uname;
			LevelOperator ret = null;
			if(uname.startsWith(LATLON_NAME)) {
				if(opt.getType() == LevelOption.TYPE_NONE) {
					ret = new LevelLatLon(dbName, machineId);
				} else {
					ret = new LevelLatLon(dbName, machineId, opt);
				}
			} else if(uname.startsWith(SEQUENCE_NAME)) {
				if(opt.getType() == LevelOption.TYPE_NONE) {
					ret = new LevelSequence(dbName, machineId);
				} else {
					ret = new LevelSequence(dbName, machineId, opt);
				}
			} else if(uname.startsWith(QUEUE_NAME)) {
				if(opt.getType() == LevelOption.TYPE_NONE) {
					ret = new LevelQueue(machineId, dbName);
				} else {
					ret = new LevelQueue(machineId, dbName, opt);
				}
			} else {
				if(opt.getType() == LevelOption.TYPE_NONE) {
					ret = new LevelMap(dbName);
				} else {
					ret = new LevelMap(dbName, opt);
				}
			}
			operatorMemManager.put(uname, ret);
			nameMemManager.put(name, uname);
			return ret;
		} finally {
			LevelBuffer.clearBuffer(null, valBuf);
		}
	}

	/**
	 * オブジェクトの取得.
	 * 
	 * @param name
	 * @return
	 */
	public LevelOperator get(String name) {
		boolean readUnlockFlag = false;
		rwLock.readLock().lock();
		try {
			closeCheck();
			String uname = nameMemManager.get(name);
			if (uname == null) {
				rwLock.readLock().unlock();
				readUnlockFlag = true;
				rwLock.writeLock().lock();
				try {
					return _get(name);
				} finally {
					rwLock.writeLock().unlock();
				}
			}
			return operatorMemManager.get(uname);
		} finally {
			if(!readUnlockFlag) {
				rwLock.readLock().unlock();
			}
		}
	}
	
	/**
	 * ユニーク名からオブジェクトを取得.
	 * @param uname 対象のユニーク名を設定します.
	 * @return
	 */
	public LevelOperator getByUniqueName(String uname) {
		boolean readUnlockFlag = false;
		rwLock.readLock().lock();
		try {
			LevelOperator ret = operatorMemManager.get(uname);
			if(ret == null) {
				rwLock.readLock().unlock();
				readUnlockFlag = true;
				rwLock.writeLock().lock();
				try {
					ret = _get(getObjectName(uname));
				} finally {
					rwLock.writeLock().unlock();
				}
			}
			return ret;
		} finally {
			if(!readUnlockFlag) {
				rwLock.readLock().unlock();
			}
		}
	}
	
	/**
	 * オブジェクト名群を取得.
	 * 
	 * @param offset
	 * @param length
	 * @return
	 */
	public List<String> names() {
		return names(0, -1);
	}

	/**
	 * オブジェクト名群を取得.
	 * 
	 * @param offset
	 * @param length
	 * @return
	 */
	public List<String> names(int offset, int length) {
		rwLock.readLock().lock();
		try {
			String name;
			LevelMapIterator it = null;
			int off = _NAME_HEADER.length();
			List<String> ret = new ObjectList<String>();
			try {
				int cnt = 0;
				it = (LevelMapIterator) manager.snapshot();
				while (it.hasNext()) {
					name = (String) it.next();
					if (name.startsWith(_NAME_HEADER)) {
						if(offset <= cnt) {
							ret.add(name.substring(off));
							if(length != -1 && ret.size() >= length) {
								break;
							}
						}
						cnt ++;
					}
				}
				it.close();
				it = null;
				return ret;
			} finally {
				if (it != null) {
					it.close();
				}
			}
		} finally {
			rwLock.readLock().unlock();
		}
	}
}
