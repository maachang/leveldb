package org.maachang.leveldb;

/**
 * Lz4圧縮／解凍.
 */
public final class Lz4 {
	protected Lz4() {}

	/**
	 * 圧縮処理.
	 * 
	 * @param out
	 *            圧縮された情報を格納するJniBufferを設定します. このバッファは内部でクリアされます.
	 * @param src
	 *            圧縮対象のJniBufferを設定します.
	 * @return int 圧縮されたサイズが返却されます.
	 */
	public static final int compress(final JniBuffer out, final JniBuffer src) {
		int srcLen;
		if ((srcLen = src.position()) == 0) {
			throw new LeveldbException("There are no conditions to compress.");
		}
		final int len = JniLz4.calcMaxCompressLength(srcLen);
		if (out.length() < len) {
			out.clear(len);
		} else {
			out.clear(false);
		}
		int ret = JniLz4.compress(src.address(), srcLen, out.address());
		out.position(ret);
		return ret;
	}

	/**
	 * 解凍処理.
	 * 
	 * @param out
	 *            解凍された情報を格納するJniBufferを設定します. このバッファは内部でクリアされます.
	 * @param src
	 *            解凍対象のJniBufferを設定します.
	 * @return int 解凍されたサイズが返却されます.
	 */
	public static final int decompress(final JniBuffer out, final JniBuffer src) {
		int srcLen;
		if ((srcLen = src.position()) == 0) {
			throw new LeveldbException("There are no conditions for decompression.");
		}
		final int len = JniLz4.decompressLength(src.address());
		if (out.length() < len) {
			out.clear(len);
		} else {
			out.clear(false);
		}
		int ret = JniLz4.decompress(src.address(), srcLen, out.address());
		out.position(ret);
		return ret;
	}
}
