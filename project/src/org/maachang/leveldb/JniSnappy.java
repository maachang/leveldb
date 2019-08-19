package org.maachang.leveldb;

/**
 * [JNIラッパー]Snappy圧縮／解凍.
 */
public final class JniSnappy {
	protected JniSnappy() {
	}

	/**
	 * 圧縮バッファサイズの計算
	 * 
	 * @param len
	 *            圧縮対象のメモリサイズを設定します.
	 * @return 圧縮バッファでの最大サイズが返却されます.
	 */
	public static final int calcMaxCompressLength(int len) {
		return 32 + len + (len / 6);
	}

	/**
	 * 解凍バイナリ長の取得.
	 * 
	 * @param address
	 *            対象のアドレスを設定します.
	 * @return int バイナリ長が返却されます.
	 */
	public static int decompressLength(long address, int off, int len) {
		int i = 0;
		int ret = 0;
		do {
			ret += (JniIO.get(address, off) & 0x7f) << (i++ * 7);
		} while ((JniIO.get(address, off++) & 0x80) == 0x80);
		return ret;
	}

	/**
	 * 圧縮処理.
	 * 
	 * @param src
	 *            圧縮対象のメモリポインタを設定します.
	 * @param src_len
	 *            圧縮対象の長さを設定します.
	 * @param dest
	 *            圧縮結果のメモリポインタを設定します.
	 * @return int 圧縮結果のサイズが返却されます.
	 */
	public static int compress(long src, int src_len, long dest) {
		int[] ret = new int[1];
		jni.snappyCompress(src, src_len, dest, ret);
		return ret[0];
	}

	/**
	 * 圧縮処理.
	 * 
	 * @param out
	 *            圧縮結果のサイズが返却されます(int[ 1 ]).
	 * @param src
	 *            圧縮対象のメモリポインタを設定します.
	 * @param src_len
	 *            圧縮対象の長さを設定します.
	 * @param dest
	 *            圧縮結果のメモリポインタを設定します.
	 * @return int 圧縮結果のサイズが返却されます.
	 */
	public static int compress(int[] out, long src, int src_len, long dest) {
		jni.snappyCompress(src, src_len, dest, out);
		return out[0];
	}

	/**
	 * 解凍処理.
	 * 
	 * @param src
	 *            解凍対象のメモリポインタを設定します.
	 * @param src_len
	 *            解凍対象の長さを設定します.
	 * @param dst
	 *            解凍結果が格納されるメモリポインタを設定します.
	 * @return int 解凍結果のサイズが返却されます.<br>
	 *         また[-1]が返却された場合、解凍処理に失敗しました.
	 */
	public static int decompress(long src, int src_len, long dst) {
		int[] ret = new int[1];
		if (jni.snappyDecompress(src, src_len, dst, ret) >= 0) {
			return ret[0];
		}
		return -1;
	}

	/**
	 * 解凍処理.
	 * 
	 * @param out
	 *            解凍結果のサイズが返却されます.<br>
	 *            また[-1]が返却された場合、解凍処理に失敗しました.
	 * @param src
	 *            解凍対象のメモリポインタを設定します.
	 * @param src_len
	 *            解凍対象の長さを設定します.
	 * @param dst
	 *            解凍結果が格納されるメモリポインタを設定します.
	 */
	public static void decompress(int[] out, long src, int src_len, long dst) {
		if (out != null && out.length > 0) {
			if (jni.snappyDecompress(src, src_len, dst, out) < 0) {
				out[0] = -1;
			}
		}
	}

}
