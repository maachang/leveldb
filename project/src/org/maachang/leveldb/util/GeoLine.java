package org.maachang.leveldb.util;

/**
 * 緯度経度間の直線距離計算.
 */
public final class GeoLine {
	protected GeoLine() {
	}

	/** 緯度1メートル係数. **/
	private static final double _latitudeM = 0.0002777778 / 30.8184033702;

	/** 経度１メートル係数. **/
	private static final double _longitudeM = 0.0002777778 / 25.244958122;

	/** ベッセル楕円体(旧日本測地系). **/
	private static final double BESSEL_A = 6377397.155;
	private static final double BESSEL_E2 = 0.00667436061028297;
	private static final double BESSEL_MNUM = 6334832.10663254;

	/** GRS80(世界測地系). **/
	private static final double GRS80_A = 6378137.000;
	private static final double GRS80_E2 = 0.00669438002301188;
	private static final double GRS80_MNUM = 6335439.32708317;

	/** WGS84(GPS). **/
	private static final double WGS84_A = 6378137.000;
	private static final double WGS84_E2 = 0.00669437999019758;
	private static final double WGS84_MNUM = 6335439.32729246;

	private static final double[] _A = new double[] { BESSEL_A, GRS80_A, WGS84_A };
	private static final double[] _E2 = new double[] { BESSEL_E2, GRS80_E2, WGS84_E2 };
	private static final double[] _MNUM = new double[] { BESSEL_MNUM, GRS80_MNUM, WGS84_MNUM };

	/** ベッセル楕円体(旧日本測地系). **/
	public static final int BESSEL = 0;

	/** GRS80(世界測地系). **/
	public static final int GRS80 = 1;

	/** WGS84(GPS). **/
	public static final int WGS84 = 2;

	/** PI-180. **/
	private static final double PI_180 = Math.PI / 180.0;

	/**
	 * 厳密な直線を求める. Copyright © 2007-2012 やまだらけ
	 * http://yamadarake.jp/trdi/report000001.html This software is released under
	 * the MIT License.
	 */
	private static final double _getLatLonLine(final double srcLat, final double srcLon, final double destLat,
			final double destLon, final double a, final double e2, final double mnum) {

		final double pi180 = PI_180;
		final double my = ((srcLat + destLat) / 2.0) * pi180;
		final double dy = (srcLat - destLat) * pi180;
		final double dx = (srcLon - destLon) * pi180;

		final double s = Math.sin(my);
		final double w = Math.sqrt(1.0 - e2 * s * s);
		final double m = mnum / (w * w * w);
		final double n = a / w;

		final double dym = dy * m;
		final double dxncos = dx * n * Math.cos(my);

		return Math.sqrt(dym * dym + dxncos * dxncos);
	}

	/**
	 * 緯度経度のポイント間の直線距離（メートル）を厳密に求める. この計算では[GRS80(世界測地系)]で求められます.
	 * 
	 * @param srcLat
	 *            元の緯度を設定します.
	 * @param srcLon
	 *            元の経度を設定します.
	 * @param destLat
	 *            先の緯度を設定します.
	 * @param destLon
	 *            先の経度を設定します.
	 * @return double 距離が返却されます.
	 */
	public static double get(final double srcLat, final double srcLon, final double destLat, final double destLon) {
		return _getLatLonLine(srcLat, srcLon, destLat, destLon, GRS80_A, GRS80_E2, GRS80_MNUM);
	}

	/**
	 * 緯度経度のポイント間の直線距離（メートル）を厳密に求める.
	 * 
	 * @param type
	 *            以下の条件が指定可能です. [BESSEL] : ベッセル楕円体(旧日本測地系). [GRS80] : GRS80(世界測地系).
	 *            [WGS84] : WGS84(GPS).
	 * @param srcLat
	 *            元の緯度を設定します.
	 * @param srcLon
	 *            元の経度を設定します.
	 * @param destLat
	 *            先の緯度を設定します.
	 * @param destLon
	 *            先の経度を設定します.
	 * @return double 距離が返却されます.
	 */
	public static double get(final int type, final double srcLat, final double srcLon, final double destLat,
			final double destLon) {
		return _getLatLonLine(srcLat, srcLon, destLat, destLon, _A[type], _E2[type], _MNUM[type]);
	}

	/**
	 * メートル換算された緯度を通常の緯度に戻します.
	 * 
	 * @param lat
	 *            緯度を設定します.
	 * @return double 元の単位に計算された内容が返却されます.
	 */
	public static final double getLat(final int lat) {
		return ((double) lat * _latitudeM);
	}

	/**
	 * メートル換算された経度を通常の経度に戻します.
	 * 
	 * @param lon
	 *            経度を設定します.
	 * @return double 元の単位に計算された内容が返却されます.
	 */
	public static final double getLon(final int lon) {
		return ((double) lon * _longitudeM);
	}

	/**
	 * 緯度をメートル計算.
	 * 
	 * @param lat
	 *            緯度を設定します.
	 * @return int メートル単位に計算された情報が返却されます.
	 */
	public static final int getLat(final double lat) {
		return (int) (lat / _latitudeM);
	}

	/**
	 * 経度をメートル計算.
	 * 
	 * @param lon
	 *            経度を設定します.
	 * @return int メートル単位に計算された情報が返却されます.
	 */
	public static final int getLon(final double lon) {
		return (int) (lon / _longitudeM);
	}

	/**
	 * メートル換算された緯度経度の直線距離を計算. この処理は、厳密な[getLatLonLine]よりも精度が劣りますが、
	 * 計算速度はビット計算で求めているので、とても高速に動作します.
	 * 
	 * @param ax
	 *            中心位置の緯度を設定します.
	 * @param ay
	 *            中心位置の経度を設定します.
	 * @param bx
	 *            対象位置の緯度を設定します.
	 * @param by
	 *            対象位置の経度を設定します.
	 * @return 大まかな直線距離が返却されます.
	 */
	public static final int getFast(final double ax, final double ay, final double bx, final double by) {
		return getFast((int) (ax / _latitudeM), (int) (ay / _longitudeM), (int) (bx / _latitudeM),
				(int) (by / _longitudeM));
	}

	/**
	 * メートル換算された緯度経度の直線距離を計算. http://d.hatena.ne.jp/nowokay/20120604#1338773843 を参考.
	 * この処理は、厳密な[getLatLonLine]よりも精度が劣りますが、 計算速度はビット計算で求めているので、とても高速に動作します.
	 * 
	 * @param ax
	 *            中心位置の緯度(メートル変換されたもの)を設定します.
	 * @param ay
	 *            中心位置の経度(メートル変換されたもの)を設定します.
	 * @param bx
	 *            対象位置の緯度(メートル変換されたもの)を設定します.
	 * @param by
	 *            対象位置の経度(メートル変換されたもの)を設定します.
	 * @return 大まかな直線距離が返却されます.
	 */
	public static final int getFast(final int ax, final int ay, final int bx, final int by) {

		// 精度はあまり高めでないが、高速で近似値を計算できる.
		final int dx, dy;
		if ((dx = (ax > bx) ? ax - bx : bx - ax) < (dy = (ay > by) ? ay - by : by - ay)) {
			return (((dy << 8) + (dy << 3) - (dy << 4) - (dy << 1) + (dx << 7) - (dx << 5) + (dx << 3)
					- (dx << 1)) >> 8);
		} else {
			return (((dx << 8) + (dx << 3) - (dx << 4) - (dx << 1) + (dy << 7) - (dy << 5) + (dy << 3)
					- (dy << 1)) >> 8);
		}
	}
}
