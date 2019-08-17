package org.maachang.leveldb.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * CSV読み込み処理.
 */
public class CsvReader {
	/** CSV読み込みオブジェクト. **/
	private BufferedReader reader = null;
	/** CSV区切り文字. **/
	private String cut;
	/** 現在読み込み中の文字. **/
	private String nowLine = null;
	/** カウント **/
	private int count = -1;
	/** eof **/
	private boolean eof = true;

	/**
	 * コンストラクタ.
	 */
	public CsvReader() {

	}

	/**
	 * コンストラクタ.
	 * 
	 * @param name
	 *            対象のファイル名を設定します.
	 * @param charset
	 *            対象のキャラクタセットを設定します.
	 * @param cut
	 *            CSV区切り文字を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public CsvReader(String name, String charset, String cut) throws Exception {
		this.open(name, charset, cut);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param r
	 *            Readerオブジェクトを設定します.
	 * @param cut
	 *            CSV区切り文字を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public CsvReader(Reader r, String cut) throws Exception {
		this.open(r, cut);
	}

	/** デストラクタ. **/
	protected void finalize() throws Exception {
		this.close();
	}

	/**
	 * ファイルオープン.
	 * 
	 * @param name
	 *            対象のファイル名を設定します.
	 * @param charset
	 *            対象のキャラクタセットを設定します.
	 * @param cut
	 *            CSV区切り文字を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public void open(String name, String charset, String cut) throws Exception {
		if (!FileUtil.isFile(name)) {
			throw new IOException("指定ファイル名[" + name + "]の内容は存在しません");
		}
		if (charset == null || charset.length() <= 0) {
			charset = "UTF8";
		}
		if (cut == null || cut.length() <= 0) {
			cut = ",";
		}
		if (isOpen()) {
			close();
		}
		this.reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(name), charset));
		this.cut = cut;
		this.count = 0;
		this.eof = false;
	}

	/**
	 * ファイルオープン.
	 * 
	 * @param r
	 *            Readerオブジェクトを設定します.
	 * @param cut
	 *            CSV区切り文字を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public void open(Reader r, String cut) throws Exception {
		if (cut == null || cut.length() <= 0) {
			cut = ",";
		}
		if (isOpen()) {
			close();
		}
		if (r instanceof BufferedReader) {
			this.reader = (BufferedReader) r;
		} else {
			this.reader = new BufferedReader(r);
		}
		this.cut = cut;
		this.count = 0;
		this.eof = false;
	}

	/**
	 * ファイルクローズ.
	 */
	public void close() {
		if (this.reader != null) {
			try {
				this.reader.close();
			} catch (Exception e) {
			}
		}
		this.reader = null;
		this.nowLine = null;
		this.eof = true;
	}

	/**
	 * 次の行を取得.
	 * 
	 * @return boolean [false]の場合は、情報が存在しません.
	 * @exception Exception
	 *                例外.
	 */
	public boolean next() throws Exception {
		if (!isOpen()) {
			return false;
		}
		String s;
		while (true) {
			if ((s = this.reader.readLine()) == null) {
				this.eof = true;
				return false;
			}
			// if( ( s = s.trim() ).length() <= 0 ) {
			// continue ;
			// }
			this.nowLine = s;
			this.count++;
			break;
		}
		return true;
	}

	/**
	 * 文字配列で、CSV取得.
	 * 
	 * @return String[] CSV区切り情報が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public String[] getArray() throws Exception {
		if (!isOpen()) {
			return null;
		}
		if (this.nowLine != null) {
			return (String[]) getCsvArray(true, this.nowLine, this.cut);
		}
		return null;
	}

	/**
	 * 文字配列で、CSV取得.
	 * 
	 * @return Object[] CSV区切り情報が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public Object[] getObjects() throws Exception {
		if (!isOpen()) {
			return null;
		}
		if (this.nowLine != null) {
			return (Object[]) getCsvArray(false, this.nowLine, this.cut);
		}
		return null;
	}

	/**
	 * リストで、CSV取得.
	 * 
	 * @return List<String> CSV区切り情報が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public List<String> get() throws Exception {
		if (!isOpen()) {
			return null;
		}
		if (this.nowLine != null) {
			return getCsv(this.nowLine, this.cut);
		}
		return null;
	}

	/**
	 * プレーンデータを取得.
	 * 
	 * @return String プレーンデータが返されます.
	 */
	public String getString() throws Exception {
		if (!isOpen()) {
			return null;
		}
		return this.nowLine;
	}

	/**
	 * 現在読み込んだカウント値を取得.
	 * 
	 * @return int 現在までの読み込み情報数が返されます.
	 */
	public int count() {
		return this.count;
	}

	/**
	 * 現在オープンされているかチェック.
	 * 
	 * @return boolean [true]の場合、オープン中です.
	 */
	public boolean isOpen() {
		return (reader != null);
	}

	/**
	 * 最終ラインに到達しているかチェック.
	 * 
	 * @return boolean [true]の場合、最終ラインに到達しています.
	 */
	public boolean isEOF() {
		return eof;
	}

	/**
	 * CSVを区切ります. <BR>
	 * 
	 * @param line
	 *            １行の内容を設定します.
	 * @param cutCode
	 *            区切り文字を設定します.
	 * @return String[] 区切られた内容が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final String[] getCsvArray(String line, String cutCode)
			throws Exception {
		return (String[]) getCsvArray(true, line, cutCode);
	}

	/** CSVを区切ります. **/
	private static final Object getCsvArray(boolean mode, String line,
			String cutCode) throws Exception {
		if (line == null || line.length() <= 0) {
			return (mode) ? new String[0] : new Object[0];
		}
		if (cutCode == null || cutCode.length() <= 0) {
			cutCode = ",";
		}
		char c;
		int cote = -1;
		int len = line.length();
		int s = 0;
		boolean yen = false;
		char cut = cutCode.charAt(0);
		String x;
		OList<String> tmp = new OList<String>();
		for (int i = 0; i < len; i++) {
			c = line.charAt(i);
			if (cote != -1) {
				if (!yen && c == cote) {
					cote = -1;
				}
			} else if (c == cut) {
				if (s == i) {
					tmp.add("");
				} else {
					x = line.substring(s, i).trim();
					if (x.indexOf("\"") == 0 || x.indexOf("\'") == 0) {
						x = x.substring(1, x.length() - 1).trim();
					}
					tmp.add(x);
				}
				s = i + 1;
			} else if (!yen && (c == '\'' || c == '\"')) {
				cote = c;
			}
			if (c == '\\') {
				yen = true;
			} else {
				yen = false;
			}
		}
		if (s >= len) {
			tmp.add("");
		} else {
			x = line.substring(s, len).trim();
			if (x.indexOf("\"") == 0 || x.indexOf("\'") == 0) {
				x = x.substring(1, x.length() - 1).trim();
			}
			tmp.add(x);
		}
		len = tmp.size();
		if (mode) {
			String[] ret = new String[len];
			Object[] strList = tmp.toArray();
			for (int i = 0; i < len; i++) {
				ret[i] = (String) strList[i];
			}
			strList = null;
			tmp.clear();
			tmp = null;
			return ret;
		} else {
			return tmp.toArray();
		}
	}

	/**
	 * CSVを区切ります. <BR>
	 * 
	 * @param line
	 *            １行の内容を設定します.
	 * @param cutCode
	 *            区切り文字を設定します.
	 * @return List<String> 区切られた内容が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final List<String> getCsv(String line, String cutCode)
			throws Exception {
		if (line == null || line.length() <= 0) {
			return new ArrayList<String>();
		}
		if (cutCode == null || cutCode.length() <= 0) {
			cutCode = ",";
		}
		char c;
		int cote = -1;
		int len = line.length();
		int s = 0;
		boolean yen = false;
		char cut = cutCode.charAt(0);
		String x;
		List<String> ret = new ArrayList<String>();
		for (int i = 0; i < len; i++) {
			c = line.charAt(i);
			if (cote != -1) {
				if (!yen && c == cote) {
					cote = -1;
				}
			} else if (c == cut) {
				if (s == i) {
					ret.add("");
				} else {
					x = line.substring(s, i).trim();
					if (x.indexOf("\"") == 0 || x.indexOf("\'") == 0) {
						x = x.substring(1, x.length() - 1).trim();
					}
					ret.add(x);
				}
				s = i + 1;
			} else if (!yen && (c == '\'' || c == '\"')) {
				cote = c;
			}
			if (c == '\\') {
				yen = true;
			} else {
				yen = false;
			}
		}
		if (s >= len) {
			ret.add("");
		} else {
			x = line.substring(s, len).trim();
			if (x.indexOf("\"") == 0 || x.indexOf("\'") == 0) {
				x = x.substring(1, x.length() - 1).trim();
			}
			ret.add(x);
		}
		return ret;
	}
}
