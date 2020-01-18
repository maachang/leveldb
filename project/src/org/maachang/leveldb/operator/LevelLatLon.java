package org.maachang.leveldb.operator;

import java.util.List;
import java.util.NoSuchElementException;

import org.maachang.leveldb.JniBuffer;
import org.maachang.leveldb.JniIO;
import org.maachang.leveldb.LevelBuffer;
import org.maachang.leveldb.LevelId;
import org.maachang.leveldb.LevelOption;
import org.maachang.leveldb.LevelValues;
import org.maachang.leveldb.Leveldb;
import org.maachang.leveldb.LeveldbException;
import org.maachang.leveldb.LeveldbIterator;
import org.maachang.leveldb.Time12SequenceId;
import org.maachang.leveldb.WriteBatch;
import org.maachang.leveldb.types.TwoKey;
import org.maachang.leveldb.util.FixedArray;
import org.maachang.leveldb.util.GeoLine;
import org.maachang.leveldb.util.GeoQuadKey;

/**
 * 緯度経度での範囲検索を行うLeveldb.
 */
public class LevelLatLon extends LevelIndexOperator {
	protected int type;
	protected Time12SequenceId sequenceId;
	protected int machineId;
	protected Object minKey;
	
	/**
	 * オペレータタイプ.
	 * @return int オペレータタイプが返却されます.
	 */
	@Override
	public int getOperatorType() {
		return LEVEL_LAT_LON;
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param machineId
	 *            マシンIDを設定します.
	 */
	public LevelLatLon(String name, int machineId) {
		this(name, machineId, null);
	}

	/**
	 * コンストラクタ.
	 * writeBatchを無効にして生成します.
	 * 
	 * @param name
	 *            対象のデータベース名を設定します.
	 * @param machineId
	 *            マシンIDを設定します.
	 * @param option
	 *            Leveldbオプションを設定します.
	 *            オプションのタイプは１キーを設定することで、緯度経度のセカンドキーになります.
	 *            [LevelOption.TYPE_NONE]を設定することで、シーケンスIDがセットされます.
	 */
	public LevelLatLon(String name, int machineId, LevelOption option) {
		int type = LevelOption.checkType(option.getType());
		if(type != LevelOption.TYPE_NONE) {
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
		super.init(null, db, true, false);
		this.type = option.getType();
		this.minKey = getMinKey(type);
		this.machineId = machineId;
		
		// インデックス初期化.
		super.initIndex(null);
	}
	
	/**
	 * コンストラクタ.
	 * writeBatchを有効にして生成します.
	 * 
	 * @param latlon 親となるオペレータを設定します.
	 */
	public LevelLatLon(LevelLatLon latlon) {
		// leveldbをクローズせずwriteBatchで処理する.
		super.init(latlon, latlon.leveldb, false, true);
		this.sequenceId = latlon.sequenceId;
		this.type = latlon.getOption().getType();
		this.minKey = latlon.minKey;
		this.machineId = latlon.machineId;
		
		// インデックス初期化処理.
		super.initIndex(latlon);
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
		if (value != null && value instanceof LevelOperator) {
			throw new LeveldbException("LevelOperator element cannot be set for the element.");
		}
		byte[] seqId = null;
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			// シーケンスIDがセカンドキーである場合.
			if(sequenceId != null) {
				// セカンドキーが空の場合.
				if(secKey == null) {
					// 新しいシーケンスIDを設定.
					seqId = sequenceId.next();
					secKey = seqId;
				// 有効なシーケンスIDがセカンドキーで設定されている場合.
				} else if(secKey instanceof byte[] || secKey instanceof String) {
					// シーケンスIDが文字列の場合は、バイナリ変換.
					if(secKey instanceof String) {
						secKey = Time12SequenceId.toBinary((String)secKey);
					}
					// バイナリサイズが不正な場合.
					if(((byte[])secKey).length != Time12SequenceId.ID_LENGTH) {
						throw new LeveldbException("Second key is not set correctly.");
					}
				// 無効なシーケンスIDが設定されている場合.
				} else {
					throw new LeveldbException("Second key is not set correctly.");
				}
			}
			keyBuf = LevelBuffer.key(type, qk, secKey);
			if (value instanceof JniBuffer) {
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, (JniBuffer) value);
				} else {
					leveldb.put(keyBuf, (JniBuffer) value);
				}
				// インデックス処理.
				if(!indexEmpty()) {
					LevelBuffer.clearBuffer(keyBuf, null);
					keyBuf = null;
					super.putIndex(qk, secKey, value);
				}
			} else {
				valBuf = LevelBuffer.value(value);
				if(writeBatchFlag) {
					writeBatch().put(keyBuf, valBuf);
				} else {
					leveldb.put(keyBuf, valBuf);
				}
				// インデックス処理.
				if(!indexEmpty()) {
					LevelBuffer.clearBuffer(keyBuf, valBuf);
					keyBuf = null; valBuf = null;
					super.putIndex(qk, secKey, value);
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
	private final JniBuffer _getKey(long qk, Object secKey)
		throws Exception {
		if(sequenceId != null) {
			// 有効なシーケンスIDがセカンドキーで設定されている場合.
			if(secKey != null && (secKey instanceof byte[] || secKey instanceof String)) {
				// シーケンスIDが文字列の場合は、バイナリ変換.
				if(secKey instanceof String) {
					secKey = Time12SequenceId.toBinary((String)secKey);
				}
				// バイナリサイズが不正な場合.
				if(((byte[])secKey).length != Time12SequenceId.ID_LENGTH) {
					throw new LeveldbException("Second key is not set correctly.");
				}
			// 無効なシーケンスIDが設定されている場合.
			} else {
				throw new LeveldbException("Second key is not set correctly.");
			}
		}
		return LevelBuffer.key(type, qk, secKey);
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
	 * @param secKey
	 * @return
	 */
	public boolean remove(long qk, Object secKey) {
		checkClose();
		JniBuffer keyBuf = null;
		Object v = null;
		try {
			final boolean idxFlg = !indexEmpty();
			if(idxFlg) {
				v = get(qk, secKey);
			}
			keyBuf = _getKey(qk, secKey);
			if(writeBatchFlag) {
				WriteBatch b = writeBatch();
				b.remove(keyBuf);
				if(idxFlg) {
					LevelBuffer.clearBuffer(keyBuf, null);
					keyBuf = null;
					super.removeIndex(qk, secKey, v);
				}
				return true;
			}
			boolean ret = leveldb.remove(keyBuf);
			if(idxFlg && ret) {
				LevelBuffer.clearBuffer(keyBuf, null);
				keyBuf = null;
				super.removeIndex(qk, secKey, v);
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
	 * @param secKey
	 * @return
	 */
	public boolean contains(long qk, Object secKey) {
		checkClose();
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			keyBuf = _getKey(qk, secKey);
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
	 * @param secKey
	 * @return
	 */
	public boolean getBuffer(JniBuffer buf, double lat, double lon, Object secKey) {
		return getBuffer(buf, GeoQuadKey.create(lat, lon), secKey);
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
			keyBuf = _getKey(qk, secKey);
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
	 * @param secKey
	 * @return
	 */
	public Object get(double lat, double lon, Object secKey) {
		return get(GeoQuadKey.create(lat, lon), secKey);
	}
	
	/**
	 * 指定キー情報に対する要素を取得.
	 * @param qk
	 * @param secKey
	 * @return
	 */
	public Object get(long qk, Object secKey) {
		checkClose();
		JniBuffer valBuf = null;
		try {
			valBuf = LevelBuffer.value();
			if (getBuffer(valBuf, qk, secKey)) {
				return LevelValues.decode(valBuf);
			}
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(null, valBuf);
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
	 * セカンドキーにシーケンスID発行かチェック.
	 * @return boolean [true]の場合、セカンドキーはシーケンスIDです.
	 */
	public boolean isSecondKeyBySequenceId() {
		return sequenceId != null;
	}
	
	/**
	 * マシンIDを取得.
	 * 
	 * @return int マシンIDが返却されます.
	 */
	public int getMachineId() {
		return machineId;
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
	 * リスト検索用snapshotを取得.
	 * @return
	 */
	public LevelQKListIterator snapshot() {
		return snapshot(false, null, null);
	}
	
	/**
	 * リスト検索用snapshotを取得.
	 * @param reverse
	 * @return
	 */
	public LevelQKListIterator snapshot(boolean reverse) {
		return snapshot(reverse, null, null);
	}
	
	/**
	 * リスト検索用snapshotを取得.
	 * @param lat
	 * @param lon
	 * @param secKey
	 * @return
	 */
	public LevelQKListIterator snapshot(double lat, double lon, Object secKey) {
		return snapshot(false, lat, lon, secKey);
	}
	
	/**
	 * リスト検索用snapshotを取得.
	 * @param qk
	 * @param secKey
	 * @return
	 */
	public LevelQKListIterator snapshot(Long qk, Object secKey) {
		return snapshot(false, qk, secKey);
	}
	
	/**
	 * リスト検索用snapshotを取得.
	 * @param reverse
	 * @param lat
	 * @param lon
	 * @param secKey
	 * @return
	 */
	public LevelQKListIterator snapshot(boolean reverse, double lat, double lon, Object secKey) {
		return snapshot(reverse, GeoQuadKey.create(lat, lon), secKey);
	}
	
	/**
	 * リスト検索用snapshotを取得.
	 * @param reverse
	 * @param qk
	 * @param secKey
	 * @return
	 */
	public LevelQKListIterator snapshot(boolean reverse, Long qk, Object secKey) {
		checkClose();
		LevelQKListIterator ret = null;
		try {
			ret = new LevelQKListIterator(reverse, this, leveldb.snapshot());
			return _search(ret, qk, secKey);
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
	
	/**
	 * リスト検索用iteratorを取得.
	 * @return
	 */
	public LevelQKListIterator iterator() {
		return iterator(false, null, null);
	}
	
	/**
	 * リスト検索用iteratorを取得.
	 * @param reverse
	 * @return
	 */
	public LevelQKListIterator iterator(boolean reverse) {
		return iterator(reverse, null, null);
	}
	
	/**
	 * リスト検索用iteratorを取得.
	 * @param reverse
	 * @param lat
	 * @param lon
	 * @param secKey
	 * @return
	 */
	public LevelQKListIterator iterator(double lat, double lon, Object secKey) {
		return iterator(false, lat, lon, secKey);
	}
	
	/**
	 * リスト検索用iteratorを取得.
	 * @param reverse
	 * @param qk
	 * @param secKey
	 * @return
	 */
	public LevelQKListIterator iterator(Long qk, Object secKey) {
		return iterator(false, qk, secKey);
	}
	
	/**
	 * リスト検索用iteratorを取得.
	 * @param reverse
	 * @param lat
	 * @param lon
	 * @param secKey
	 * @return
	 */
	public LevelQKListIterator iterator(boolean reverse, double lat, double lon, Object secKey) {
		return iterator(reverse, GeoQuadKey.create(lat, lon), secKey);
	}
	
	/**
	 * リスト検索用iteratorを取得.
	 * @param reverse
	 * @param qk
	 * @param secKey
	 * @return
	 */
	public LevelQKListIterator iterator(boolean reverse, Long qk, Object secKey) {
		checkClose();
		LevelQKListIterator ret = null;
		try {
			ret = new LevelQKListIterator(reverse, this, leveldb.iterator());
			return _search(ret, qk, secKey);
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
	
	// 指定キーで検索処理.
	protected LevelQKListIterator _search(LevelQKListIterator ret, Long qk, Object secKey)
		throws Exception {
		if(qk != null) {
			Leveldb.search(ret.itr, ret.reverse, type, _getKey(qk, secKey), null);
		} else if(ret.reverse) {
			ret.itr.last();
		}
		return ret;
	}
	
	/**
	 * リスト検索用LevelQuadKeyDb用Iterator.
	 */
	public class LevelQKListIterator extends LevelIterator<Object[], Object> {
		LevelLatLon db;
		LeveldbIterator itr;

		/**
		 * コンストラクタ.
		 * 
		 * @param reverse
		 *            逆カーソル移動させる場合は[true]
		 * @param db
		 *            対象の親オブジェクトを設定します.
		 * @param itr
		 *            LeveldbIteratorオブジェクトを設定します.
		 */
		LevelQKListIterator(boolean reverse, LevelLatLon db, LeveldbIterator itr) {
			this.db = db;
			this.itr = itr;
			this.reverse = reverse;
		}

		// ファイナライズ.
//		protected void finalize() throws Exception {
//			close();
//		}

		/**
		 * クローズ処理.
		 */
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

		/**
		 * 次の情報が存在するかチェック.
		 * 
		 * @return boolean [true]の場合、存在します.
		 */
		public boolean hasNext() {
			if (db.isClose() || itr == null || !itr.valid()) {
				close();
				return false;
			}
			return true;
		}

		/**
		 * 次の要素を取得.
		 * 
		 * @return String 次の要素が返却されます.
		 */
		public Object next() {
			if (db.isClose() || itr == null || !itr.valid()) {
				close();
				throw new NoSuchElementException();
			}
			JniBuffer keyBuf = null;
			JniBuffer valBuf = null;
			try {
				keyBuf = LevelBuffer.key();
				valBuf = LevelBuffer.value();
				itr.key(keyBuf);
				itr.value(valBuf);
				Object key = LevelId.get(db.type, keyBuf);
				Object val = LevelValues.decode(valBuf);
				LevelBuffer.clearBuffer(keyBuf, valBuf);
				keyBuf = null;
				valBuf = null;
				if(reverse) {
					itr.before();
				} else {
					itr.next();
				}
				if(!itr.valid()) {
					close();
				}
				TwoKey tk = (TwoKey)key;
				if(db.sequenceId != null) {
					this.resultKey = new Object[] {tk.get(0), Time12SequenceId.toString((byte[])tk.get(1))};
				} else {
					this.resultKey = new Object[] {tk.get(0), tk.get(1)};
				}
				return val;
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
	 * 範囲検索用snapshotを取得.
	 * @param lat
	 * @param lon
	 * @param distance 検索範囲（メートル）を設定します.
	 * @return
	 */
	public LeveQKSearchIterator snapshot(double lat, double lon, int distance) {
		return snapshot(GeoQuadKey.create(lat, lon), distance);
	}
	
	/**
	 * 範囲検索用snapshotを取得.
	 * @param qk
	 * @param distance 検索範囲（メートル）を設定します.
	 * @return
	 */
	public LeveQKSearchIterator snapshot(long qk, int distance) {
		checkClose();
		return new LeveQKSearchIterator(qk, minKey, distance, this, true);
	}
	
	/**
	 * 範囲検索用iteratorを取得.
	 * @param lat
	 * @param lon
	 * @param distance 検索範囲（メートル）を設定します.
	 * @return
	 */
	public LeveQKSearchIterator iterator(double lat, double lon, int distance) {
		return iterator(GeoQuadKey.create(lat, lon), distance);
	}
	
	/**
	 * 範囲検索用iteratorを取得.
	 * @param qk
	 * @param distance 検索範囲（メートル）を設定します.
	 * @return
	 */
	public LeveQKSearchIterator iterator(long qk, int distance) {
		checkClose();
		return new LeveQKSearchIterator(qk, minKey, distance, this, false);
	}
	
	/**
	 * 範囲検索用LevelQuadKeyDb用Iterator.
	 */
	public class LeveQKSearchIterator extends LevelIterator<List<Object>, Object> {
		protected LevelLatLon db;
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
		protected Object nowValue;
		
		protected LeveQKSearchIterator(long qk, Object secKey, int distance, LevelLatLon db, boolean snapshot) {
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
			_next();
		}
		
//		@Override
//		protected void finalize() throws Exception {
//			close();
//		}
		
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
		public boolean isClose() {
			return itr == null || itr.isClose();
		}
		
		private void _next() {
			db.checkClose();
			if(endFlag) {
				close();
				nowKey = null;
				nowValue = null;
			}
			Object key;
			long nowQk;
			double[] latLon = new double[2];
			boolean nextRead = nowCount == -1;
			JniBuffer keyBuf = null;
			JniBuffer valBuf = null;
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
							nowKey = null;
							nowValue = null;
							return;
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
					// 取得した位置情報は、distanceの範囲内かチェック.
					GeoQuadKey.latLon(latLon, nowQk);
					if(GeoLine.getFast(latM, lonM, GeoLine.getLat(latLon[0]), GeoLine.getLon(latLon[1])) > distance) {
						// 次の情報でリトライ.
						itr.next();
						continue;
					}
					valBuf = LevelBuffer.value();
					itr.value(valBuf);
					// 今回の情報をセット.
					this.nowKey = key;
					this.nowValue = LevelValues.decode(valBuf);
					LevelBuffer.clearBuffer(keyBuf, valBuf);
					keyBuf = null;
					valBuf = null;
					// 次の情報を読み込む.
					itr.next();
					return;
				}
			} catch (LeveldbException le) {
				throw le;
			} catch (Exception e) {
				throw new LeveldbException(e);
			} finally {
				LevelBuffer.clearBuffer(keyBuf, valBuf);
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
		public Object next() {
			if(db.isClose() || endFlag) {
				close();
				throw new NoSuchElementException();
			}
			Object key = this.nowKey;
			Object value = this.nowValue;
			_next();
			if(key != null) {
				TwoKey tk = (TwoKey)key;
				if(db.sequenceId != null) {
					this.resultKey = new FixedArray<Object>(
						new Object[] {tk.get(0), Time12SequenceId.toString((byte[])tk.get(1))});
				} else {
					this.resultKey = new FixedArray<Object>(
						new Object[] {tk.get(0), tk.get(1)});
				}
				return value;
			}
			return null;
		}
	}
}
