package org.maachang.leveldb;

/**
 * Leveldbバッチカーソル. WriteBatchで書き込み中の情報を 閲覧できる.
 */
public class WriteBatchCursor {

	// この機能は、Leveldbを拡張して処理している。
	// よって、以下の３つのメソッドの追加が必要となる.
	//
	// <db/write_batch.cc>
	// // [START]add maachang.
	// WriteBatch::WriteBatch( const size_t size ) {
	// Clear( size ) ;
	// }
	// void WriteBatch::Clear( const size_t size ) {
	// rep_.clear() ;
	// rep_.reserve(size) ;
	// rep_.resize(kHeader) ;
	// }
	// const char* WriteBatch::Values() {
	// return &(rep_[0]) ;
	// }
	// size_t WriteBatch::ValuesSize() {
	// return rep_.size() ;
	// }
	// [END]add maachang.
	//
	// </include/leveldb/write_batch.h>
	//
	// // [START]add maachang.
	// WriteBatch( const size_t size );
	// void Clear( const size_t size );
	// const char* Values() ;
	// size_t ValuesSize() ;
	// // [END]add maachang.
	//
	// それぞれに追加が必須.
	//

	/** 処理タイプ : PUT. **/
	public static final int PUT = 0x01;

	/** 処理タイプ : Delete. **/
	public static final int DELETE = 0x00;

	/** ヘッダサイズ. **/
	private static final int HEADER_LENGTH = 12;

	// LeveldbのWriteBatchは、以下のように格納されている.
	// ヘッダ要素.
	// 0 - 7 : シーケンスID.
	// 8 - 11 : 全体長.
	// 12 - : 各要素.
	//
	// 各要素は、以下のように格納されている.
	// [Put][KeyLength][Key...][ValueLength][Value...]
	// [Delete][KeyLength][Key...]
	// (1) : PUT or Delete.
	//
	// PUTの場合、
	// (1-5) : Keyの長さ.
	// Keyの長さ分のBinary.
	// (1-5) : 要素の長さ.
	// 要素の長さ分のBinary.
	//
	// Deleteの場合.
	// (1-5) : Keyの長さ.
	// Keyの長さ分のBinary.
	//
	// 長さの情報は、7bit区切りで格納され、条件が続く場合は[0x80]のフラグが
	// 付加されているので、これらを計算してセットすることで、本来の長さが
	// 取得できる.
	//

	private long addr;
	private long seqId;
	private int length;
	private int max;

	private int position;

	private int mode;
	private JniBuffer keyBuf;
	private JniBuffer valueBuf;
	private int count;
	private final int[] pointer = new int[1];

	/**
	 * コンストラクタ.
	 * 
	 * @param batch
	 *            対象のWriteBatchオブジェクトを設定します.
	 * @exception Exception
	 *                例外.
	 */
	protected WriteBatchCursor(WriteBatch batch) throws Exception {
		this.addr = jni.leveldb_wb_values(batch.addr);
		this.max = jni.leveldb_wb_values_size(batch.addr);

		if (max < HEADER_LENGTH) {
			length = 0;
			seqId = 0L;
		} else {
			seqId = JniIO.getLongE(addr, 0);
			length = JniIO.getIntE(addr, 8);
		}
		this.position = HEADER_LENGTH;
		this.count = 0;

		keyBuf = LevelBuffer.key();
		valueBuf = LevelBuffer.value();
		mode = -1;
	}

	/** デストラクタ. **/
	protected final void finalize() throws Exception {
		clear();
	}

	/**
	 * 情報クリア.
	 */
	public void clear() {
		if (keyBuf != null) {
			keyBuf.clear(true);
			keyBuf = null;
		}
		if (valueBuf != null) {
			valueBuf.clear();
			valueBuf = null;
		}
		addr = 0L;
		seqId = 0L;
		length = 0;
		mode = -1;
	}

	/**
	 * 次の情報を読み込む.
	 * 
	 * @return boolean [true]の場合、情報が取得できました.
	 */
	public boolean next() {
		if (count >= length) {
			return false;
		}
		int p = position;
		int[] pp = pointer;

		// 処理モードを取得.
		mode = JniIO.get(addr, p);
		p++;
		pp[0] = p;

		// Key長を取得.
		int len = varint32(pp, addr);
		if (len == -1) {
			throw new LeveldbException("[key]varint32の長さが不正です:" + p);
		}
		p = pp[0];

		// Key内容をコピー.
		keyBuf.clear(len);
		JniIO.memcpy(keyBuf.address(), addr + p, len);
		keyBuf.position(len);
		p += len;

		// 処理モードがPUTの場合は、要素を取得.
		if (mode == PUT) {

			// 要素の長さを取得.
			pp[0] = p;
			len = varint32(pp, addr);
			if (len == -1) {
				throw new LeveldbException("[value]varint32の長さが不正です:" + p);
			}
			p = pp[0];

			// 要素の内容をコピー.
			valueBuf.clear(len);
			JniIO.memcpy(valueBuf.address(), addr + p, len);
			valueBuf.position(len);
			p += len;
		}

		// ポインタをセット.
		position = p;

		// 取得カウントを１インクリメント.
		count++;
		return true;
	}

	/** 長さを取得. **/
	private static final int varint32(final int[] p, final long addr) {
		int ret = 0;
		int b;
		for (int shift = 0; shift <= 28; shift += 7) {
			b = JniIO.get(addr, p[0]++);
			if ((b & 0x00000080) == 0x00000080) {
				ret |= (b & 127) << shift;
			} else {
				return ret | (b << shift);
			}
		}
		return -1;
	}

	/**
	 * 次の情報が読み込み可能かチェック.
	 * 
	 * @return boolean [true]の場合、読み込み可能です.
	 */
	public boolean hasNext() {
		return count < length;
	}

	/**
	 * 対象のモードを取得.
	 * 
	 * @return int モードが返却されます. [PUT]の場合は、モードはPutです. [DELETE]の場合は、モードはDeleteです.
	 */
	public int getMode() {
		return mode;
	}

	/**
	 * Key情報を取得.
	 * 
	 * @return JniBuffer Key情報が格納されたJniBufferが返却されます.
	 */
	public JniBuffer getKey() {
		return keyBuf;
	}

	/**
	 * Value情報を取得.
	 * 
	 * @return JniBuffer Value情報が格納されたJniBufferが返却されます.
	 */
	public JniBuffer getValue() {
		return valueBuf;
	}

	/**
	 * 現在のカウントを取得.
	 * 
	 * @return int 現在の取得カウントが返却されます.
	 */
	public final int getCount() {
		return count - 1;
	}

	/**
	 * データ長を取得.
	 * 
	 * @return int データ長が返却されます.
	 */
	public int getMax() {
		return length;
	}

	/**
	 * シーケンスIDを取得.
	 * 
	 * @return long シーケンスIDが返却されます.
	 */
	public long getSequenceId() {
		return seqId;
	}

	/**
	 * WriteBatchバイナリ長を取得.
	 * 
	 * @return int バイナリ長が返却されます.
	 */
	public int getBinaryLength() {
		return max;
	}
}
