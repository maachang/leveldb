package org.maachang.leveldb;

import java.util.NoSuchElementException;

import org.maachang.leveldb.types.TwoKey;
import org.maachang.leveldb.util.GeoLine;
import org.maachang.leveldb.util.GeoQuadKey;

/**
 * 緯度経度での範囲検索を行うLeveldb.
 */
public class LevelQuadKeyDb extends CommitRollback {
	protected int type;
	protected Time12SequenceId sequenceId = null;
	protected Object minKey = null;
	
	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param machineId
	 *            マシンIDを設定します.
	 */
	public LevelQuadKeyDb(String name, int machineId) {
		this(name, machineId, -1, null);
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param machineId
	 *            マシンIDを設定します.
	 * @param type
	 *            オプションのタイプは１キーを設定することで、緯度経度のセカンドキーになります.
	 *            -1を設定することで、シーケンスIDがセットされます.
	 */
	public LevelQuadKeyDb(String name, int machineId, int type) {
		this(name, machineId, type, null);
	}

	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param machineId
	 *            マシンIDを設定します.
	 * @param type
	 *            オプションのタイプは１キーを設定することで、緯度経度のセカンドキーになります.
	 *            -1を設定することで、シーケンスIDがセットされます.
	 * @param option
	 *            Leveldbオプションを設定します.
	 *            オプションのタイプは無視されて、typeパラメータがセットされます.
	 */
	public LevelQuadKeyDb(String name, int machineId, int type, LevelOption option) {
		type = LevelOption.checkType(type);
		if(type != -1) {
			if(LevelOption.typeMode(type) != 1 || type == LevelOption.TYPE_MULTI) {
				throw new LeveldbException("Only the \"one key\" condition can be set for the second key.");
			}
			type = LevelOption.convertType("number64-" + LevelOption.stringType(type));
			option.setType(type);
		} else {
			sequenceId = new Time12SequenceId(machineId);
			option = LevelOption.create(LevelOption.TYPE_N64_BIN);
		}
		Leveldb db = new Leveldb(name, option);
		// leveldbをクローズしてwriteBatchで処理しない.
		super.init(db, true, false);
		this.type = option.getType();
		this.minKey = getMinKey(type);
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを有効にして生成します.
	 * 
	 * @param db
	 */
	public LevelQuadKeyDb(LevelQuadKeyDb db) {
		// leveldbをクローズせずwriteBatchで処理する.
		super.init(db.leveldb, false, true);
		this.sequenceId = db.sequenceId;
		this.type = db.getOption().getType();
		this.minKey = db.minKey;
	}
	
	// LeveQuadKeyIterator にわたすセカンドキーを取得.
	private static final Object getMinKey(int type) {
		int secType = LevelOption.getSecondKeyType(type);
		if(secType != -1) {
			switch(secType) {
			case LevelOption.TYPE_STRING: return "";
			case LevelOption.TYPE_NUMBER32: return Integer.MIN_VALUE;
			case LevelOption.TYPE_NUMBER64: return Long.MIN_VALUE;
			case LevelOption.TYPE_FREE: return new byte[0];
			}
		}
		return null;
	}
	
	/**
	 * 緯度、経度をセット.
	 * @param lat
	 * @param lon
	 * @param value
	 * @return quadKeyが返却されます.
	 * @return Object[]
	 *         [0]: quadKeyが設定されます.
	 *         [1]: セカンドキーが存在しない定義の場合は、シーケンスIDが返却されます.
	 */
	public Object[] put(double lat, double lon, Object value) {
		return put(lat, lon, null, value);
	}
	
	/**
	 * 緯度、経度をセット.
	 * @param lat
	 * @param lon
	 * @param secKey
	 * @param value
	 * @return quadKeyが返却されます.
	 * @return Object[]
	 *         [0]: quadKeyが設定されます.
	 *         [1]: セカンドキーが存在しない定義の場合は、シーケンスIDが返却されます.
	 */
	public Object[] put(double lat, double lon, Object secKey, Object value) {
		return put(GeoQuadKey.create(lat, lon), secKey, value);
	}
	
	/**
	 * QuadKeyをセット.
	 * @param qk
	 * @param value
	 * @return Object[]
	 *         [0]: quadKeyが設定されます.
	 *         [1]: セカンドキーが存在しない定義の場合は、シーケンスIDが返却されます.
	 */
	public Object[] put(long qk, Object value) {
		return put(qk, null, value);
	}
	
	/**
	 * QuadKeyをセット.
	 * @param qk
	 * @param secKey
	 * @param value
	 * @return Object[]
	 *         [0]: quadKeyが設定されます.
	 *         [1]: セカンドキーが存在しない定義の場合は、シーケンスIDが返却されます.
	 */
	public Object[] put(long qk, Object secKey, Object value) {
		checkClose();
		if (value != null && value instanceof LevelMap) {
			throw new LeveldbException("LevelMap element cannot be set for the element.");
		}
		byte[] seqId = null;
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			// シーケンスIDがセカンドキーである場合.
			if(sequenceId != null) {
				// シーケンスIDが空の場合は、新しいシーケンスIDを発行.
				if(secKey == null) {
					seqId = sequenceId.next();
					secKey = seqId;
				// シーケンスIDが文字列の場合は、バイナリ変換.
				} else if(secKey instanceof String) {
					secKey = Time12SequenceId.toBinary((String)secKey);
				}
			}
			keyBuf = LevelBuffer.key(type, qk, secKey);
			if (value instanceof JniBuffer) {
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, (JniBuffer) value);
				} else {
					leveldb.put(keyBuf, (JniBuffer) value);
				}
			} else {
				valBuf = LevelBuffer.value(value);
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, valBuf);
				} else {
					leveldb.put(keyBuf, valBuf);
				}
			}
			// セカンドキーがシーケンスIDの場合は、文字変換で返却.
			if(sequenceId != null) {
				return new Object[] {qk, Time12SequenceId.toString(seqId)};
			}
			return new Object[] {qk, secKey};
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, valBuf);
		}
	}
	
	// キー情報を取得.
	private final JniBuffer getKey(long qk, Object secKey)
		throws Exception {
		if(sequenceId != null && secKey instanceof String) {
			secKey = Time12SequenceId.toBinary((String)secKey);
		}
		return LevelBuffer.key(type, qk, secKey);
	}
	
	/**
	 * データ削除.
	 * @param lat
	 * @param lon
	 * @return
	 */
	public boolean remove(double lat, double lon) {
		return remove(lat, lon, null);
	}
	
	/**
	 * データ削除.
	 * @param lat
	 * @param lon
	 * @param secKey
	 * @return
	 */
	public boolean remove(double lat, double lon, Object secKey) {
		return remove(GeoQuadKey.create(lat, lon), secKey);
	}
	
	/**
	 * データ削除.
	 * @param qk
	 * @return
	 */
	public boolean remove(long qk) {
		return remove(qk, null);
	}
	
	/**
	 * データ削除.
	 * @param qk
	 * @param secKey
	 * @return
	 */
	public boolean remove(long qk, Object secKey) {
		checkClose();
		JniBuffer keyBuf = null;
		try {
			keyBuf = getKey(qk, secKey);
			if(writeBatchFlag) {
				WriteBatch b = writeBatch();
				b.remove(keyBuf);
				return true;
			}
			return leveldb.remove(keyBuf);
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, null);
		}
	}
	
	/**
	 * 情報存在確認.
	 * @param lat
	 * @param lon
	 * @return
	 */
	public boolean contains(double lat, double lon) {
		return contains(lat, lon, null);
	}
	
	/**
	 * 情報存在確認.
	 * @param lat
	 * @param lon
	 * @param secKey
	 * @return
	 */
	public boolean contains(double lat, double lon, Object secKey) {
		return contains(GeoQuadKey.create(lat, lon), secKey);
	}
	
	/**
	 * 情報存在確認.
	 * @param qk
	 * @return
	 */
	public boolean contains(long qk) {
		return contains(qk, null);
	}
	
	/**
	 * 情報存在確認.
	 * @param qk
	 * @param secKey
	 * @return
	 */
	public boolean contains(long qk, Object secKey) {
		checkClose();
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			keyBuf = getKey(qk, secKey);
			if(writeBatchFlag) {
				LeveldbIterator snapshot = getSnapshot();
				snapshot.seek(keyBuf);
				if (snapshot.valid()) {
					valBuf = LevelBuffer.value();
					snapshot.key(valBuf);
					return JniIO.equals(keyBuf.address(), keyBuf.position(),
						valBuf.address(), valBuf.position());
				}
				return false;
			} else {
				valBuf = LevelBuffer.value();
				int res = leveldb.get(valBuf, keyBuf);
				return res != 0;
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, valBuf);
		}
	}
	
	/**
	 * 情報存在確認.
	 * @param buf
	 * @param lat
	 * @param lon
	 * @return
	 */
	public boolean getBuffer(JniBuffer buf, double lat, double lon) {
		return getBuffer(buf, lat, lon, null);
	}
	
	/**
	 * 情報存在確認.
	 * @param buf
	 * @param lat
	 * @param lon
	 * @param secKey
	 * @return
	 */
	public boolean getBuffer(JniBuffer buf, double lat, double lon, Object secKey) {
		return getBuffer(buf, GeoQuadKey.create(lat, lon), secKey);
	}
	
	/**
	 * 情報存在確認.
	 * @param buf
	 * @param qk
	 * @return
	 */
	public boolean getBuffer(JniBuffer buf, long qk) {
		return getBuffer(buf, qk, null);
	}
	
	/**
	 * 指定キー情報に対する要素を取得.
	 * @param buf
	 * @param qk
	 * @param secKey
	 * @return
	 */
	public boolean getBuffer(JniBuffer buf, long qk, Object secKey) {
		checkClose();
		boolean ret = false;
		JniBuffer keyBuf = null;
		try {
			keyBuf = getKey(qk, secKey);
			if(writeBatchFlag) {
				LeveldbIterator snapshot = getSnapshot();
				snapshot.seek(keyBuf);
				// 条件が存在する場合.
				if (snapshot.valid()) {
					snapshot.key(buf);
					// 対象キーが正しい場合.
					if (JniIO.equals(keyBuf.address(), keyBuf.position(),
						buf.address(), buf.position())) {
						snapshot.value(buf);
						ret = true;
					}
				}
			}
			if (leveldb.get(buf, keyBuf) != 0) {
				ret = true;
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, null);
		}
		return ret;
	}
	
	/**
	 * 指定キー情報に対する要素を取得.
	 * @param lat
	 * @param lon
	 * @return
	 */
	public Object get(double lat, double lon) {
		return get(lat, lon, null);
	}
	
	/**
	 * 指定キー情報に対する要素を取得.
	 * @param lat
	 * @param lon
	 * @param secKey
	 * @return
	 */
	public Object get(double lat, double lon, Object secKey) {
		return get(GeoQuadKey.create(lat, lon), secKey);
	}
	
	/**
	 * 指定キー情報に対する要素を取得.
	 * @param qk
	 * @return
	 */
	public Object get(long qk) {
		return get(qk, null);
	}
	
	/**
	 * 指定キー情報に対する要素を取得.
	 * @param qk
	 * @param secKey
	 * @return
	 */
	public Object get(long qk, Object secKey) {
		checkClose();
		JniBuffer buf = null;
		try {
			buf = LevelBuffer.value();
			if (getBuffer(buf, qk, secKey)) {
				return LevelValues.decode(buf);
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(null, buf);
		}
		return null;
	}
	
	/**
	 * 情報が空かチェック.
	 * 
	 * @return boolean データが空の場合[true]が返却されます.
	 */
	public boolean isEmpty() {
		checkClose();
		if(writeBatchFlag) {
			try {
				LeveldbIterator snapshot = getSnapshot();
				snapshot.first();
				if (snapshot.valid()) {
					return false;
				}
				return true;
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			}
		}
		return leveldb.isEmpty();
	}
	
	/**
	 * 現在オープン中のLeveldbパス名を取得.
	 * 
	 * @return String Leveldbパス名が返却されます.
	 */
	public String getPath() {
		checkClose();
		return leveldb.getPath();
	}
	
	/**
	 * snapshotを取得.
	 * @param lat
	 * @param lon
	 * @param distance 検索範囲（メートル）を設定します.
	 * @return
	 */
	public LeveQuadKeyIterator snapshot(double lat, double lon, int distance) {
		return snapshot(lat, lon, distance);
	}
	
	/**
	 * snapshotを取得.
	 * @param qk
	 * @param distance 検索範囲（メートル）を設定します.
	 * @return
	 */
	public LeveQuadKeyIterator snapshot(long qk, int distance) {
		checkClose();
		return new LeveQuadKeyIterator(qk, minKey, distance, this, true);
	}
	
	/**
	 * iteratorを取得.
	 * @param lat
	 * @param lon
	 * @param distance 検索範囲（メートル）を設定します.
	 * @return
	 */
	public LeveQuadKeyIterator iterator(double lat, double lon, int distance) {
		return iterator(lat, lon, distance);
	}
	
	/**
	 * iteratorを取得.
	 * @param qk
	 * @param distance 検索範囲（メートル）を設定します.
	 * @return
	 */
	public LeveQuadKeyIterator iterator(long qk, int distance) {
		checkClose();
		return new LeveQuadKeyIterator(qk, minKey, distance, this, false);
	}
	
	/**
	 * LevelQuadKeyDb用Iterator.
	 */
	public class LeveQuadKeyIterator implements LevelIterator<Object[]> {
		protected LevelQuadKeyDb db;
		protected LeveldbIterator itr;
		protected int type;
		protected int latM;
		protected int lonM;
		protected int distance;
		protected Object secKey;
		protected long[] list;
		protected int nowCount;
		protected boolean endFlag;
		protected Object nowKey;
		
		protected LeveQuadKeyIterator(long qk, Object secKey, int distance, LevelQuadKeyDb db, boolean snapshot) {
			double[] latLon = GeoQuadKey.latLon(qk);
			long[] searchList = GeoQuadKey.searchCode(
				GeoQuadKey.getDetail(distance), latLon[0], latLon[1]);
			
			this.db = db;
			this.itr = snapshot ? db.leveldb.snapshot() : db.leveldb.iterator();
			this.type = db.type;
			this.latM = GeoLine.getLat(latLon[0]);
			this.lonM = GeoLine.getLat(latLon[1]);
			this.distance = distance;
			this.secKey = secKey;
			
			this.list = searchList;
			this.nowCount = -1;
			this.endFlag = false;
			this.nowKey = _next();
		}
		
		@Override
		protected void finalize() throws Exception {
			close();
		}
		
		@Override
		public void close() {
			if(itr != null) {
				itr.close();
				itr = null;
			}
			list = null;
			endFlag = true;
		}
		
		@Override
		public boolean isReverse() {
			return false;
		}
		
		private Object _next() {
			db.checkClose();
			if(endFlag) {
				close();
				return null;
			}
			Object key;
			long nowQk;
			double[] latLon = new double[2];
			boolean nextRead = true;
			JniBuffer keyBuf = null;
			try {
				keyBuf = LevelBuffer.key();
				while(true) {
					// データの終端か、次の枠を読みこむ場合.
					if(!itr.valid() || nextRead) {
						nextRead = false;
						nowCount ++;
						// 検索結果の終了.
						if(nowCount >= 9) {
							close();
							return null;
						}
						LevelId.buf(type, keyBuf, list[nowCount << 1], secKey);
						itr.seek(keyBuf);
						keyBuf.position(0);
						if(!itr.valid()) {
							nextRead = true;
							continue;
						}
					}
					// データの取得.
					itr.key(keyBuf);
					key = LevelId.get(type, keyBuf);
					keyBuf.position(0);
					nowQk = (Long)((TwoKey)key).get(0);
					// 終端を検出した場合.
					if(nowQk > list[(nowCount << 1) + 1]) {
						nextRead = true;
						continue;
					}
					// 次の情報を読み込む.
					itr.next();
					// 取得した位置情報は、distanceの範囲内かチェック.
					GeoQuadKey.latLon(latLon, nowQk);
					if(GeoLine.getFast(latM, lonM, GeoLine.getLat(latLon[0]), GeoLine.getLon(latLon[1])) > distance) {
						continue;
					}
					// 今回取得したデータを返却.
					LevelBuffer.clearBuffer(keyBuf, null);
					keyBuf = null;
					return key;
				}
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			} finally {
				LevelBuffer.clearBuffer(keyBuf, null);
			}
		}

		@Override
		public boolean hasNext() {
			if(db.isClose()) {
				close();
			}
			return !endFlag;
		}

		@Override
		public Object[] next() {
			if(db.isClose() || endFlag) {
				close();
				throw new NoSuchElementException();
			}
			Object ret = this.nowKey;
			this.nowKey = _next();
			if(ret != null) {
				TwoKey tk = (TwoKey)ret;
				if(db.sequenceId != null) {
					return new Object[] {tk.get(0), Time12SequenceId.toString((byte[])tk.get(1))};
				}
				return new Object[] {tk.get(0), tk.get(1)};
			}
			return null;
		}
	}
}
