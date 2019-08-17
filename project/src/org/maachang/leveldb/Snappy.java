package org.maachang.leveldb;

/**
 * Snappy圧縮／解凍.
 */
public final class Snappy {

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
			throw new LeveldbException("圧縮対象の条件は存在しません");
		}
		final int len = JniSnappy.calcMaxCompressLength(srcLen);
		if (out.length() < len) {
			out.clear(len);
		} else {
			out.clear(false);
		}
		int ret = JniSnappy.compress(src.address(), srcLen, out.address());
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
			throw new LeveldbException("解凍対象の条件は存在しません");
		}
		final int len = JniSnappy.decompressLength(src.address(), 0, srcLen);
		if (out.length() < len) {
			out.clear(len);
		} else {
			out.clear(false);
		}
		int ret = JniSnappy.decompress(src.address(), srcLen, out.address());
		out.position(ret);
		return ret;
	}

}
