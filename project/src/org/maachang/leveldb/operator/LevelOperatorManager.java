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
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.Time12SequenceId;
import org.maachang.leveldb.operator.LevelMap.LevelMapIterator;
import org.maachang.leveldb.util.Converter;
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

	// ユニークオペレータ名のキー名の先頭名.
	private static final String _UNIQUE_HEADER = "@u_";

	// オペレータ名とユニークオペレータ名の先頭名.
	private static final String _NAME_HEADER = "@n_";
	
	// 緯度経度管理オペレータ名.
	private static final String LATLON_NAME = "@ll@";
	
	// シーケンスIDオペレータ名.
	private static final String SEQUENCE_NAME = "@sq@";
	
	// キューオペレータ名.
	private static final String QUEUE_NAME = "@qu@";
	
	// MAPオペレータ名.
	private static final String MAP_NAME = "@mp@";
	
	// ベースパス.
	private String basePath = "./";
	
	// マシンID.
	private int machineId = 0;

	// マシンIDやテーブル名を管理する leveldb.
	private LevelMap manager = null;
	
	// ユニーク名を作成するTime12SequenceId.
	private Time12SequenceId uniqueManager = null;

	// オペレータ情報管理.
	// key = ユニークオペレータ名.
	private Map<String, LevelOperator> operatorMemManager = new ConcurrentHashMap<String, LevelOperator>();

	// オペレータ名管理.
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
		// フォルダが存在しない場合は作成する.
		if(!FileUtil.isDir(path) || !FileUtil.isDir(path + OPERATOR_PATH)) {
			FileUtil.mkdir(path + OPERATOR_PATH);
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
	//protected void finalize() throws Exception {
	//	this.close();
	//}

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
	 * オペレータがクローズしているかチェック.
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

	// 指定オペレータ名からユニークオペレータ名を取得.
	protected final String getUniqueName(String name) {
		return (String) manager.get(_NAME_HEADER + name);
	}

	// 指定ユニーク名からオペレータ名を取得.
	protected final String getObjectName(String uname) {
		String key, value;
		String target = _UNIQUE_HEADER + uname;
		LevelMapIterator it = manager.iterator();
		while(it.hasNext()) {
			it.next();
			if((key = (String)it.getKey()).startsWith(_NAME_HEADER)) {
				value = (String)manager.get(key);
				if(target.equals(value)) {
					return key.substring(_NAME_HEADER.length());
				}
			}
		}
		return null;
	}

	// オペレータを作成.
	protected void _createOperator(String name, String uniqueOrigin, int objectType, LevelOption opt) {
		JniBuffer valBuf = null;
		try {
			String uname;
			switch(objectType) {
			case LevelOperator.LEVEL_LAT_LON:
				uname = LATLON_NAME + uniqueOrigin;
				break;
			case LevelOperator.LEVEL_SEQUENCE:
				uname = SEQUENCE_NAME + uniqueOrigin;
				break;
			case LevelOperator.LEVEL_QUEUE:
				uname = QUEUE_NAME + uniqueOrigin;
				break;
			default:
				uname = MAP_NAME + uniqueOrigin;
				break;
			}
			// 名前を保存.
			manager.put(_NAME_HEADER + name, uname);
			// ユニーク名に、生成オプションを保存.
			if(opt == null) {
				// タイプなしで作成.
				opt = LevelOption.create(LevelOption.TYPE_NONE);
			}
			valBuf = LevelBuffer.value();
			opt.toBuffer(valBuf);
			manager.put(_UNIQUE_HEADER + uname, valBuf);
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(null, valBuf);
		}
	}
	
	// 指定オペレータ削除処理.
	protected boolean _delete(String name, String uname) {
		try {
			// オペレータがオープンの場合は、強制クローズ.
			LevelOperator obj = operatorMemManager.get(uname);
			if (obj != null) {
				obj.deleteComplete();
				obj = null;
			}
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
	}

	// ユニーク名からオペレータタイプを取得.
	private int _unameByOperatorType(String uname) {
		if(uname.startsWith(LATLON_NAME)) {
			return LevelOperator.LEVEL_LAT_LON;
		} else if(uname.startsWith(SEQUENCE_NAME)) {
			return LevelOperator.LEVEL_SEQUENCE;
		} else if(uname.startsWith(QUEUE_NAME)) {
			return LevelOperator.LEVEL_QUEUE;
		} else {
			return LevelOperator.LEVEL_MAP;
		}
	}
	
	// オペレータが存在するかチェック.
	private boolean _isOperator(String name) {
		if (!manager.containsKey(_NAME_HEADER + name)) {
			return false;
		}
		return true;
	}
	
	// オペレータをロード.
	private LevelOperator _loadOperator(String name) {
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
			
			switch(_unameByOperatorType(uname)) {
			case LevelOperator.LEVEL_LAT_LON:
				ret = new LevelLatLon(dbName, machineId, opt);
				break;
			case LevelOperator.LEVEL_SEQUENCE:
				ret = new LevelSequence(dbName, machineId, opt);
				break;
			case LevelOperator.LEVEL_QUEUE:
				ret = new LevelQueue(machineId, dbName, opt);
				break;
			case LevelOperator.LEVEL_MAP:
				if(opt.getType() == LevelOption.TYPE_NONE) {
					opt.setType(LevelOption.TYPE_STRING);
				}
				ret = new LevelMap(dbName, opt);
				break;
			}
			// キャッシュにセット.
			operatorMemManager.put(uname, ret);
			nameMemManager.put(name, uname);
			return ret;
		} finally {
			LevelBuffer.clearBuffer(null, valBuf);
		}
	}

	/**
	 * Mapオペレータを生成.
	 * 
	 * @param name
	 *            オペレータ名を設定します.
	 * @param opt
	 *            オペレータ用のLeveldbOption を設定します.
	 * @return [true]の場合、生成成功です.
	 */
	public boolean createMap(String name, LevelOption opt) {
		return create(name, LevelOperator.LEVEL_MAP, opt);
	}
	
	/**
	 * 緯度経度用オペレータオペレータを生成.
	 * 
	 * @param name
	 *            オペレータ名を設定します.
	 * @param opt
	 *            オペレータ用のLeveldbOption を設定します.
	 * @return [true]の場合、生成成功です.
	 */
	public boolean createLatLon(String name, LevelOption opt) {
		return create(name, LevelOperator.LEVEL_LAT_LON, opt);
	}
	
	/**
	 * シーケンスオペレータオペレータを生成.
	 * 
	 * @param name
	 *            オペレータ名を設定します.
	 * @param opt
	 *            オペレータ用のLeveldbOption を設定します.
	 * @return [true]の場合、生成成功です.
	 */
	public boolean createSequence(String name, LevelOption opt) {
		return create(name, LevelOperator.LEVEL_SEQUENCE, opt);
	}

	/**
	 * キューオペレータオペレータを生成.
	 * 
	 * @param name
	 *            オペレータ名を設定します.
	 * @param opt
	 *            オペレータ用のLeveldbOption を設定します.
	 * @return [true]の場合、生成成功です.
	 */
	public boolean createQueue(String name, LevelOption opt) {
		return create(name, LevelOperator.LEVEL_QUEUE, opt);
	}

	// ユニーク名を作成.
	private final String _getUniqueName() {
		// ユニーク名をBase64で生成するが、この時の名前に対して「/」が入るので
		// オープンに失敗してしまう。
		// なので、/ の文字列を - に変更する.
		String ret = Time12SequenceId.toString(uniqueManager.next());
		ret = Converter.changeString(ret, "/", "-");
		return ret;
	}

	/**
	 * 新しいオペレータを生成.
	 * 
	 * @param name
	 *            オペレータ名を設定します.
	 * @param objectType
	 *            オペレータタイプを設定します.
	 * @param opt
	 *            オペレータ用のLeveldbOption を設定します.
	 * @return [true]の場合、生成成功です.
	 */
	public boolean create(String name, int objectType, LevelOption opt) {
		rwLock.writeLock().lock();
		try {
			closeCheck();
			// 既に同一名の情報が存在する場合.
			if (manager.containsKey(_NAME_HEADER + name)) {
				return false;
			}
			_createOperator(name, _getUniqueName(), objectType, opt);
			return true;
		} finally {
			rwLock.writeLock().unlock();
		}
	}

	/**
	 * 指定オペレータを削除.
	 * 
	 * @param name
	 * @return
	 */
	public boolean delete(String name) {
		rwLock.writeLock().lock();
		try {
			closeCheck();
			// 対象の情報が存在しない場合.
			if (!manager.containsKey(_NAME_HEADER + name)) {
				return false;
			}
			String uname = getUniqueName(name);
			if (uname == null) {
				return false;
			}
			return _delete(name, uname);
		} finally {
			rwLock.writeLock().unlock();
		}
	}
	
	/**
	 * 指定オペレータの名前変更.
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
	
	/**
	 * オペレータが登録されてるかチェック.
	 * 
	 * @param name
	 * @return
	 */
	public boolean contains(String name) {
		rwLock.readLock().lock();
		try {
			return _isOperator(name);
		} finally {
			rwLock.readLock().unlock();
		}
	}
	
	/**
	 * オペレータを取得.
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
					return _loadOperator(name);
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
	 * ユニーク名からオペレータを取得.
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
					ret = _loadOperator(getObjectName(uname));
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
	 * 登録オペレータ名群を取得.
	 * 
	 * @return
	 */
	public List<String> names() {
		return names(0, -1);
	}

	/**
	 * 登録オペレータ名群を取得.
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
					it.next();
					name = (String)it.getKey();
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
	
	/**
	 * 登録オペレータ数を取得.
	 * 
	 * @return
	 */
	public int size() {
		rwLock.readLock().lock();
		try {
			LevelMapIterator it = null;
			try {
				int ret = 0;
				it = (LevelMapIterator) manager.snapshot();
				while (it.hasNext()) {
					it.next();
					if (((String)it.getKey()).startsWith(_NAME_HEADER)) {
						ret ++;
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
