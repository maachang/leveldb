package org.maachang.leveldb.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 * CSV書き込み処理.
 */
public class CsvWriter {

	/** 書き込みオブジェクト. **/
	private BufferedWriter writer = null;
	/** 区切り文字. **/
	private String cut = null;
	/** 書き込みバッファ. **/
	private StringBuilder buf = null;
	/** カウント. **/
	private int count = -1;

	/**
	 * コンストラクタ.
	 */
	public CsvWriter() {

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
	public CsvWriter(String name, String charset, String cut) throws Exception {
		this.open(name, charset, cut);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param newFile
	 *            [true]の場合、新規ファイルで作成します.
	 * @param name
	 *            対象のファイル名を設定します.
	 * @param charset
	 *            対象のキャラクタセットを設定します.
	 * @param cut
	 *            CSV区切り文字を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public CsvWriter(boolean newFile, String name, String charset, String cut)
			throws Exception {
		this.open(newFile, name, charset, cut);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param w
	 *            Writerオブジェクトを設定します.
	 * @param cut
	 *            CSV区切り文字を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public CsvWriter(Writer w, String cut) throws Exception {
		this.open(w, cut);
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
		open(true, name, charset, cut);
	}

	/**
	 * ファイルオープン.
	 * 
	 * @param newFile
	 *            [true]の場合、新規ファイルで作成します.
	 * @param name
	 *            対象のファイル名を設定します.
	 * @param charset
	 *            対象のキャラクタセットを設定します.
	 * @param cut
	 *            CSV区切り文字を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public void open(boolean newFile, String name, String charset, String cut)
			throws Exception {
		if (charset == null || charset.length() <= 0) {
			charset = "UTF8";
		}
		if (cut == null || cut.length() <= 0) {
			cut = ",";
		}
		if (isOpen()) {
			close();
		}
		this.writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(name, !newFile), charset));
		this.cut = cut;
		this.count = 0;
	}

	/**
	 * ファイルオープン.
	 * 
	 * @param w
	 *            Writerオブジェクトを設定します.
	 * @param cut
	 *            CSV区切り文字を設定します.
	 * @exception Exception
	 *                例外.
	 */
	public void open(Writer w, String cut) throws Exception {
		if (cut == null || cut.length() <= 0) {
			cut = ",";
		}
		if (isOpen()) {
			close();
		}
		if (w instanceof BufferedWriter) {
			this.writer = (BufferedWriter) w;
		} else {
			this.writer = new BufferedWriter(w);
		}
		this.cut = cut;
		this.count = 0;
	}

	/**
	 * ファイルクローズ.
	 */
	public void close() {
		if (this.writer != null) {
			try {
				this.writer.close();
			} catch (Exception e) {
			}
		}
		this.writer = null;
		this.buf = null;
	}

	/**
	 * 現在のバッファを一旦クリア.
	 */
	public void reset() {
		buf = null;
	}

	/**
	 * Boolean情報のバッファ追加.
	 * 
	 * @param value
	 *            対象の情報を設定します.
	 */
	public void add(Boolean value) {
		if (buf == null) {
			buf = new StringBuilder();
		} else {
			buf.append(cut);
		}
		if (value != null) {
			buf.append(value);
		}
	}

	/**
	 * 数値情報のバッファ追加.
	 * 
	 * @param value
	 *            対象の情報を設定します.
	 */
	public void add(Number value) {
		if (buf == null) {
			buf = new StringBuilder();
		} else {
			buf.append(cut);
		}
		if (value != null) {
			buf.append(value);
		}
	}

	/**
	 * 文字情報のバッファ追加.
	 * 
	 * @param value
	 *            対象の情報を設定します.
	 */
	public void add(String value) {
		if (buf == null) {
			buf = new StringBuilder();
		} else {
			buf.append(cut);
		}
		if (value != null) {
			buf.append("\"").append(value).append("\"");
		}
	}

	/**
	 * バッファに追加した情報をファイル出力.
	 * 
	 * @return int 書き込み行カウント数が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public int write() throws Exception {
		if (buf == null) {
			throw new IOException("書き込み対象のバッファは存在しません");
		}
		String s = buf.append("\n").toString();
		buf = null;
		writer.write(s, 0, s.length());
		return ++count;
	}

	/**
	 * 書き込み情報のFlush.
	 * 
	 * @exception Exception
	 *                例外.
	 */
	public void flush() throws Exception {
		writer.flush();
	}

	/**
	 * 書き込み行カウント数を取得.
	 * 
	 * @return int 書き込み行カウント数が返却されます.
	 */
	public int count() {
		return count;
	}

	/**
	 * 現在オープンされているかチェック.
	 * 
	 * @return boolean [true]の場合、オープン中です.
	 */
	public boolean isOpen() {
		return (writer != null);
	}

}
