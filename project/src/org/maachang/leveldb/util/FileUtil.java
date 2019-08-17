package org.maachang.leveldb.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ファイルユーティリティ.
 */
public final class FileUtil {
	protected FileUtil() {
	}

	/**
	 * OSファイルスペース.
	 */
	public static final String FILE_SP = System.getProperty("file.separator");

	/**
	 * ファイル名の存在チェック.
	 * 
	 * @param name
	 *            対象のファイル名を設定します.
	 * @return boolean [true]の場合、ファイルは存在します.
	 */
	public static final boolean isFile(String name) {
		File file = new File(name);
		return (file.exists() && !file.isDirectory());
	}

	/**
	 * ディレクトリ名の存在チェック.
	 * 
	 * @param name
	 *            対象のディレクトリ名を設定します.
	 * @return boolean [true]の場合、ディレクトリは存在します.
	 */
	public static final boolean isDir(String name) {
		File file = new File(name);
		return (file.exists() && file.isDirectory());
	}

	/**
	 * ディレクトリを生成.
	 * 
	 * @param name
	 *            対象のファイル名を設定します.
	 * @return boolean [true]の場合、存在するか成功しました.
	 */
	public static final boolean mkdir(String name) {
		File file = new File(name);
		if (file.exists() && file.isDirectory()) {
			return true;
		}
		return file.mkdirs();
	}

	/**
	 * ファイル名のフルパスを取得.
	 * 
	 * @param name
	 *            対象のファイル名を設定します.
	 * @return String フルパス名が返却されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final String getFullPath(String name) throws Exception {
		File f = new File(name);
		String s = f.getCanonicalPath();
		if (s.indexOf("\\") != -1) {
			s = Utils.changeString(s, "\\", "/");
		}
		if (!s.startsWith("/")) {
			s = "/" + s;
		}
		return s;
	}

	/**
	 * 対象パスのフォルダ名のみ取得.
	 * 
	 * @param path
	 *            対象のパスを設定します.
	 * @return String フォルダ名が返却されます. [null]が返却された場合、フォルダは存在しません.
	 * @exception Exception
	 *                例外.
	 */
	public static final String getDirectory(String path) throws Exception {
		if (!path.startsWith("/")) {
			path = FileUtil.getFullPath(path);
		}
		if (FileUtil.isFile(path)) {
			int p = path.lastIndexOf("\\");
			int p2 = path.lastIndexOf("/");
			if (p == -1) {
				if (p2 != -1) {
					p = p2;
				}
			} else if (p2 != -1 && p < p2) {
				p = p2;
			}
			if (p == -1) {
				if (!path.endsWith("/")) {
					return path + "/";
				}
				return path;
			}
			path = path.substring(0, p);
		}
		if (!FileUtil.isDir(path)) {
			int p = path.lastIndexOf("/");
			if (p == -1) {
				throw new IOException("指定フォルダは存在しません[" + path + "]");
			}
			path = path.substring(0, p);
		}
		if (FileUtil.isDir(path)) {
			if (!path.endsWith("/")) {
				return path + "/";
			}
			return path;
		}
		throw new IOException("指定フォルダは存在しません[" + path + "]");
	}

	/**
	 * 指定ディレクトリ以下の指定拡張子が一致するファイル一覧を取得.
	 * 
	 * @param dir
	 *            対象のディレクトリ名を設定します.
	 * @param plus
	 *            対象の拡張子を設定します.<br>
	 *            [null]の場合、拡張子を識別せずに取得します.
	 * @return String[] ファイル名一覧が返されます.
	 */
	public static final String[] getDirByUseFileName(String dir, String plus)
			throws Exception {
		if (dir == null || (dir = dir.trim()).length() <= 0) {
			throw new IllegalArgumentException("引数は不正です");
		}
		if (plus == null || (plus = plus.trim()).length() <= 0) {
			plus = null;
		} else if (plus.startsWith(".") == false) {
			plus = "." + plus;
		}
		List<String> lst = new ArrayList<String>();
		readDirToFileList(lst, dir, dir, plus);
		if (lst.size() > 0) {
			int len = lst.size();
			String[] ret = new String[len];
			for (int i = 0; i < len; i++) {
				ret[i] = lst.get(i);
			}
			lst = null;
			Arrays.sort(ret);
			return ret;
		}
		return null;
	}

	/**
	 * 指定ディレクトリ以下のディレクトリ名を取得.
	 */
	private static final void readDirToFileList(List<String> out, String base,
			String dir, String plus) throws Exception {
		File fp = new File(dir);
		String[] names = fp.list();
		fp = null;
		if (names != null && names.length > 0) {
			int len = names.length;
			for (int i = 0; i < len; i++) {
				if (names[i] == null
						|| (names[i] = names[i].trim()).length() <= 0) {
					continue;
				}
				String name = new StringBuilder().append(dir).append(FILE_SP)
						.append(names[i]).toString();
				if (FileUtil.isDir(name)) {
					readDirToFileList(out, base, name, plus);
				} else if (plus == null
						|| Utils.toLowerCase(name).endsWith(plus)) {
					name = name.substring(base.length(), name.length());
					if (name.indexOf("\\") != -1) {
						name = Utils.changeString(name, "\\", "/");
					}
					if (name.startsWith("/") == true) {
						name.substring(1, name.length());
					}
					out.add(name);
				}
			}
		}
	}
}
