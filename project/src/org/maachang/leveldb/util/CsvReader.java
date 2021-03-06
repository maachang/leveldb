package org.maachang.leveldb.util;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * CsvReader.
 */
public class CsvReader implements Iterator<Map<String, Object>>, Closeable, AutoCloseable {
	private FixedSearchArray<String> header;
	private String[] headerList;
	private List<String> now;
	private CsvReaderRow rowMap;

	private String cutCode;
	private BufferedReader reader;
	private boolean eof = false;
	private int count = 0;

	/**
	 * コンストラクタ.
	 * 
	 * @param n    対象のファイル名を設定します.
	 * @param c    Csvのカットコードを設定します.
	 * @throws IOException
	 */
	public CsvReader(String n, String c) throws IOException {
		this(n, "UTF8", c);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param n    対象のファイル名を設定します.
	 * @param cset 文字charsetを設定します.
	 * @param c    Csvのカットコードを設定します.
	 * @throws IOException
	 */
	public CsvReader(String n, String cset, String c) throws IOException {
		this(new BufferedReader(new InputStreamReader(new FileInputStream(n), cset)), c);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param r Readerオブジェクトを設定します.
	 * @throws IOException
	 */
	public CsvReader(Reader r) throws IOException {
		this(r, ",");
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param r Readerオブジェクトを設定します.
	 * @param c Csvのカットコードを設定します.
	 * @throws IOException
	 */
	public CsvReader(Reader r, String c) throws IOException {
		if (!(r instanceof BufferedReader)) {
			r = new BufferedReader(r);
		}
		this.reader = (BufferedReader) r;
		this.cutCode = c;
		init();
	}

	/**
	 * クローズ処理.
	 * 
	 * @exception IOException I/O例外.
	 */
	public void close() throws IOException {
		eof = true;
		header = null;
		headerList = null;
		now = null;
		rowMap = null;

		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				throw e;
			}
			reader = null;
		}
	}

	// １行のCSV情報をカット.
	private static final boolean getCsv(List<String> out, String line, String cutCode) throws IOException {
		if (line == null || line.length() <= 0) {
			return false;
		}
		int cnt = 0;
		boolean setMode = out.size() != 0;
		char c;
		int cote = -1;
		int len = line.length();
		int s = 0;
		boolean yen = false;
		char cut = cutCode.charAt(0);
		String x;
		for (int i = 0; i < len; i++) {
			c = line.charAt(i);
			if (cote != -1) {
				if (!yen && c == cote) {
					cote = -1;
				}
			} else if (c == cut) {
				if (s == i) {
					if (setMode) {
						out.set(cnt++, "");
					} else {
						out.add("");
					}
				} else {
					x = line.substring(s, i).trim();
					if (x.indexOf("\"") == 0 || x.indexOf("\'") == 0) {
						x = x.substring(1, x.length() - 1);
					}
					if (setMode) {
						out.set(cnt++, x);
					} else {
						out.add(x);
					}
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
			if (setMode) {
				out.set(cnt++, "");
			} else {
				out.add("");
			}
		} else {
			x = line.substring(s, len).trim();
			if (x.indexOf("\"") == 0 || x.indexOf("\'") == 0) {
				x = x.substring(1, x.length() - 1).trim();
			}
			if (setMode) {
				out.set(cnt++, x);
			} else {
				out.add(x);
			}

		}
		if (setMode && cnt != out.size()) {
			throw new IOException("データ数が違います:" + out.size() + "/" + cnt);
		}
		return true;
	}

	// １行の情報.
	private static final class CsvReaderRow implements ConvertMap {
		private FixedSearchArray<String> header;
		private String[] headerList;
		private List<String> rowData;

		public CsvReaderRow(FixedSearchArray<String> h, String[] lst) {
			header = h;
			headerList = lst;
		}

		public CsvReaderRow set(List<String> r) {
			rowData = r;
			return this;
		}

		private int _getParamNo(Object key) {
			if (key == null) {
				return -1;
			}
			// 数値だった場合は、番号で処理.
			if (Converter.isNumeric(key)) {
				int n = Converter.convertInt(key);
				if (n >= 0 && n < rowData.size()) {
					return n;
				}
				return -1;
			}
			int ret = header.search(key.toString());
			if(ret == -1) {
				return -1;
			}
			return ret;
		}

		@Override
		public Object get(Object key) {
			int n = _getParamNo(key);
			if (n == -1) {
				return null;
			}
			return rowData.get(n);
		}

		@Override
		public boolean containsKey(Object key) {
			return (_getParamNo(key) == -1) ? false : true;
		}

		@Override
		public int size() {
			return rowData.size();
		}

		@Override
		public String toString() {
			final int len = headerList.length;
			StringBuilder buf = new StringBuilder("{");
			for (int i = 0; i < len; i++) {
				if (i != 0) {
					buf.append(",");
				}
				String v = rowData.get(i);
				buf.append("\"").append(headerList[i]).append("\":");
				if (Converter.isNumeric(v)) {
					buf.append(v);
				} else if ("TRUE".equals(v)) {
					buf.append("true");
				} else if ("FALSE".equals(v)) {
					buf.append("false");
				} else if (v.length() == 0) {
					buf.append("null");
				} else {
					buf.append("\"").append(v).append("\"");
				}
			}
			buf.append("}");
			return buf.toString();
		}
	}

	// 初期処理.
	private final void init() throws IOException {

		// ヘッダ情報を取得.
		String line = reader.readLine();
		if (line == null) {
			throw new IOException("CSV情報の読み込みに失敗しました");
		}
		List<String> list = new ArrayList<String>();
		if (!getCsv(list, line, cutCode)) {
			throw new IOException("CSV情報の読み込みに失敗しました");
		}
		final int len = list.size();
		final String[] lst = new String[len];
		final FixedSearchArray<String> m = new FixedSearchArray<String>(len);
		for (int i = 0; i < len; i++) {
			lst[i] = list.get(i);
			m.add(list.get(i), i);
		}
		header = m;
		headerList = lst;
		now = list;
		rowMap = new CsvReaderRow(header, headerList);
	}

	/**
	 * 次のCSVデータが存在するかチェック.
	 * 
	 * @return boolean [true]の場合、存在します.
	 */
	@Override
	public boolean hasNext() {
		if (eof) {
			return false;
		}
		try {
			String line = reader.readLine();
			if (line == null) {
				eof = true;
				return false;
			}
			if (!getCsv(now, line, cutCode)) {
				eof = true;
				return false;
			}
			count++;
			return true;
		} catch (Exception e) {
			eof = true;
			return false;
		}
	}

	/**
	 * 現在の１行データ情報を取得.
	 * 
	 * @return Map Csvデータが返却されます.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> next() {
		if (eof) {
			throw new NoSuchElementException();
		}
		return rowMap.set(now);
	}

	/**
	 * 現在の行番号を取得.
	 * 
	 * @return int 現在の行番号が返却されます.
	 */
	public int getRowCount() {
		return count;
	}
	
	/**
	 * ヘッダ数を取得.
	 * @return
	 */
	public int getHeaderSize() {
		return headerList.length;
	}
	
	/**
	 * ヘッダ情報を取得.
	 * @param no
	 * @return
	 */
	public String getHeader(int no) {
		return headerList[no];
	}

	public String toString() {
		return "csvReader";
	}
}
