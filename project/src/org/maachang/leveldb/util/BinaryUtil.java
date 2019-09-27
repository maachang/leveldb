package org.maachang.leveldb.util;

/**
 * バイナリユーティリティ.
 */
public class BinaryUtil {
	private BinaryUtil() {}
	
	/**
	 * バイナリ比較.
	 * 
	 * @param a
	 *            比較元のバイナリを設定します.
	 * @param b
	 *            比較先のバイナリを設定します.
	 * @return int aが大きい場合は[1]、aが小さい場合は[-1]、同じ場合は[0].
	 */
	public static final int binaryCompareTo(byte[] a, byte[] b) {
		int aLen = a.length;
		int bLen = b.length;
		int len = aLen > bLen ? bLen : aLen;
		int ab, bb;
		for (int i = 0; i < len; i++) {
			ab = (int) a[i] & 0xff;
			bb = (int) b[i] & 0xff;
			if (ab > bb) {
				return 1;
			} else if (ab < bb) {
				return -1;
			}
		}
		if (aLen > bLen) {
			return 1;
		} else if (aLen < bLen) {
			return -1;
		}
		return 0;
	}

	/**
	 * バイナリを文字でHex表示.
	 * 
	 * @param b
	 *            対象のバイナリを設定します.
	 * @return String Hex情報が返却されます.
	 */
	public static final String binaryToHexString(byte[] b) {
		String n;
		int len = b.length;
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < len; i++) {
			if (i != 0) {
				buf.append(",");
			}
			n = Integer.toHexString(b[i] & 255);
			buf.append("00".substring(n.length())).append(n);
		}
		return buf.toString();
	}
}
