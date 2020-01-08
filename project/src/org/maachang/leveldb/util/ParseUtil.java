package org.maachang.leveldb.util;

import java.io.IOException;

/**
 * Parseユーティリティ.
 */
public final class ParseUtil {
	protected ParseUtil() {
	}

	/**
	 * コメント行を省いた情報を取得.
	 * 
	 * @param script
	 *            対象のスクリプトを設定します.
	 * @return String コメント行を除外した内容が返されます.
	 */
	public static final String cutComment(String script) throws Exception {
		if (script == null || script.length() <= 0) {
			return "";
		}
		int p;
		StringBuilder buf = new StringBuilder();
		int len = script.length();
		int cote = -1;
		int commentType = -1;
		int bef = -1;
		for (int i = 0; i < len; i++) {
			if (i != 0) {
				bef = script.charAt(i - 1);
			}
			char c = script.charAt(i);
			// コメント内の処理.
			if (commentType != -1) {
				switch (commentType) {
				case 1: // １行コメント.
					if (c == '\n') {
						buf.append(c);
						commentType = -1;
					}
					break;
				case 2: // 複数行コメント.
					if (c == '\n') {
						buf.append(c);
					} else if (len > i + 1 && c == '*' && script.charAt(i + 1) == '/') {
						i++;
						commentType = -1;
					}
					break;
				}
				continue;
			}
			// シングル／ダブルコーテーション内の処理.
			if (cote != -1) {
				if (c == cote && (char) bef != '\\') {
					cote = -1;
				}
				buf.append(c);
				continue;
			}
			// それ以外の処理.
			if (c == '/') {
				// Javascriptの正規表現の場合は処理しない.
				if ((p = javascriptRegExp(script, i, len)) != -1) {
					buf.append(script.substring(i, p + 1));
					i = p;
					continue;
				}
				if (len <= i + 1) {
					buf.append(c);
					continue;
				}
				char c2 = script.charAt(i + 1);
				if (c2 == '*') {
					commentType = 2;
					continue;
				} else if (c2 == '/') {
					commentType = 1;
					continue;
				}
			}
			// コーテーション開始.
			else if ((c == '\'' || c == '\"') && (char) bef != '\\') {
				cote = (int) (c & 0x0000ffff);
			}
			buf.append(c);
		}
		return buf.toString();
	}

	/** 指定スラッシュが、Javascript正規表現開始位置かチェック. **/
	/** -1が返却された場合は、条件が見つからないことを示します. **/
	private static final int javascriptRegExp(String str, int p, int len) {
		if (p + 1 >= len) {
			return -1;
		}
		char c = str.charAt(p + 1);
		if (c == '/' || c == '*') {
			return -1;
		}
		for (int i = p + 1, b = -1; i < len; i++) {
			c = str.charAt(i);
			if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
				return -1;
			} else if (c == '/' && b == '\\') {
				b = -1;
				continue;
			} else if (b == '/') {
				if (c == 'i' || c == 'g' || c == 'm') {
					return i;
				} else {
					return i - 1;
				}
			}
			b = c;
		}
		return -1;
	}

	/**
	 * 指定位置からの、有効文字列を取得.
	 * 
	 * @param outPos
	 *            検知情報の開始位置が返されます.
	 * @param script
	 *            対象のスクリプトを設定します.
	 * @param index
	 *            対象のインデックス値を設定します.
	 */
	public static final String parseToSingle(int[] outPos, String script, int index) {
		int len = script.length();
		if (index <= 0) {
			index = 0;
		}
		int cote = -1;
		char bef = 0;
		int pos = -1;
		boolean yenFlag = false;
		for (int i = index; i < len; i++) {
			char c = script.charAt(i);
			if (cote != -1) {
				if (bef != '\\' && cote == c) {
					cote = -1;
					yenFlag = false;
					if (pos >= 0) {
						if (outPos != null && outPos.length >= 1) {
							outPos[0] = pos;
						}
						return script.substring(pos, i + 1);
					}
				} else if (c == '\\' && bef == '\\') {
					yenFlag = true;
				} else {
					yenFlag = false;
				}
			} else if (bef != '\\' && (c == '\'' || c == '\"')) {
				if (pos >= 0) {
					if (outPos != null && outPos.length >= 1) {
						outPos[0] = pos;
					}
					return script.substring(pos, i);
				}
				cote = c;
				pos = i;
			} else if (c == ' ' || c == '　' || c == '\r' || c == '\n' || c == '\t') {
				if (pos >= 0) {
					if (outPos != null && outPos.length >= 1) {
						outPos[0] = pos;
					}
					return script.substring(pos, i);
				}
			} else if (c == '(' || c == '{' || c == '[' || c == ')' || c == '}' || c == ']' || c == ':' || c == ';') {
				if (pos >= 0) {
					if (outPos != null && outPos.length >= 1) {
						outPos[0] = pos;
					}
					return script.substring(pos, i);
				} else {
					if (outPos != null && outPos.length >= 1) {
						outPos[0] = i;
					}
					return script.substring(i, i + 1);
				}
			} else if (pos <= -1) {
				pos = i;
			}
			if (yenFlag) {
				bef = 0;
				yenFlag = false;
			} else {
				bef = c;
			}
		}
		if (outPos != null && outPos.length >= 1) {
			outPos[0] = -1;
		}
		return null;
	}

	/**
	 * 指定位置から前の、有効文字列を取得.
	 * 
	 * @param outPos
	 *            検知情報の開始位置が返されます.
	 * @param script
	 *            対象のスクリプトを設定します.
	 * @param index
	 *            対象のインデックス値を設定します.
	 */
	public static final String parseToLastOfSingle(int[] outPos, String script, int index) {
		if (index <= 0) {
			index = 0;
		}
		int cote = -1;
		char bef = 0;
		int pos = -1;
		boolean yenFlag = false;
		for (int i = index - 1; i >= 0; i--) {
			char c = script.charAt(i);
			if (cote != -1) {
				if (bef != '\\' && cote == c) {
					cote = -1;
					yenFlag = false;
					if (pos >= 0) {
						if (outPos != null && outPos.length >= 1) {
							outPos[0] = i;
						}
						return script.substring(i, pos);
					}
				} else if (c == '\\' && bef == '\\') {
					yenFlag = true;
				} else {
					yenFlag = false;
				}
			} else if (bef != '\\' && (c == '\'' || c == '\"')) {
				if (pos >= 0) {
					if (outPos != null && outPos.length >= 1) {
						outPos[0] = i + 1;
					}
					return script.substring(i + 1, pos);
				}
				cote = c;
				pos = i + 1;
			} else if (c == ' ' || c == '　' || c == '\r' || c == '\n' || c == '\t') {
				if (pos >= 0) {
					if (outPos != null && outPos.length >= 1) {
						outPos[0] = i + 1;
					}
					return script.substring(i + 1, pos);
				}
			} else if (c == '(' || c == '{' || c == '[' || c == ')' || c == '}' || c == ']' || c == ':' || c == ';') {
				if (pos >= 0) {
					if (outPos != null && outPos.length >= 1) {
						outPos[0] = i + 1;
					}
					return script.substring(i + 1, pos);
				} else {
					if (outPos != null && outPos.length >= 1) {
						outPos[0] = i;
					}
					return script.substring(i, i + 1);
				}
			} else if (pos <= -1) {
				pos = i + 1;
			}
			if (yenFlag) {
				bef = 0;
				yenFlag = false;
			} else {
				bef = c;
			}
		}
		if (outPos != null && outPos.length >= 1) {
			outPos[0] = -1;
		}
		return null;
	}

	/**
	 * 文法検知処理.
	 * <p>
	 * コーテーションや、スペース等の区切り文字を除外した、文法チェック.
	 * </p>
	 * 
	 * @param script
	 *            対象のスクリプトを設定します.
	 * @param chk
	 *            対象の区切り文字を設定します.
	 * @param index
	 *            検索開始位置を設定します.
	 * @return int -1の場合、存在しません.
	 */
	public static final int indexByParse(String script, String chk, int index) {
		int len = script.length();
		int chkLen = chk.length();
		if (len <= 0 || chkLen <= 0) {
			return -1;
		}
		if (index <= 0) {
			index = 0;
		}
		int cote = -1;
		char bef = 0;
		int pos = 0;
		boolean yenFlag = false;
		for (int i = index; i < len; i++) {
			char c = script.charAt(i);
			if (cote != -1) {
				if (bef != '\\' && cote == c) {
					cote = -1;
					yenFlag = false;
				} else if (c == '\\' && (char) bef == '\\') {
					yenFlag = true;
				} else {
					yenFlag = false;
				}
			} else if (bef != '\\' && (c == '\'' || c == '\"')) {
				cote = c;
				pos = -1;
			} else if (c == ' ' || c == '　' || c == '\r' || c == '\n' || c == '\t' || c == ';' || c == ':' || c == '('
					|| c == '{' || c == '[' || c == ')' || c == '}' || c == ']') {
				if (chkLen <= pos) {
					return i - chkLen;
				}
				if (c == ';' || c == ':' || c == '(' || c == '{' || c == '[' || c == ')' || c == '}' || c == ']') {
					if (pos >= 0 && c == chk.charAt(pos)) {
						pos++;
					} else {
						pos = 0;
					}
				} else {
					pos = 0;
				}
			} else {
				if (chkLen <= pos) {
					pos = -1;
				}
				if (pos >= 0 && c == chk.charAt(pos)) {
					pos++;
				}
			}
			if (yenFlag) {
				bef = 0;
				yenFlag = false;
			} else {
				bef = c;
			}
		}
		return -1;
	}

	/**
	 * 文法の内、コーテーションを除く連続文字列を検知.
	 * 
	 * @param base
	 *            対象のスクリプトを設定します.
	 * @param cc
	 *            対象の区切り文字を設定します.
	 * @param off
	 *            検索開始位置を設定します.
	 * @return int -1の場合、存在しません.
	 */
	public static final int indexByEquals(String base, String cc, int off) {
		int len = base.length();
		int cote = -1;
		char[] ck = cc.toCharArray();
		int cLen = ck.length;
		char bef = 0;
		boolean yenFlag = false;
		for (int i = off; i < len; i++) {
			char c = base.charAt(i);
			if (cote != -1) {
				if (bef != '\\' && c == cote) {
					yenFlag = false;
					cote = -1;
				} else if (c == '\\' && bef == '\\') {
					yenFlag = true;
				} else {
					yenFlag = false;
				}
			} else {
				if (bef != '\\' && (c == '\'' || c == '\"')) {
					cote = c;
				} else if (c == ck[0]) {
					boolean res = true;
					for (int j = 1; j < cLen; j++) {
						if (i + j >= len || ck[j] != base.charAt(i + j)) {
							res = false;
							break;
						}
					}
					if (res == true) {
						return i;
					}
				}
			}
			if (yenFlag) {
				yenFlag = false;
				bef = 0;
			} else {
				bef = c;
			}
		}
		return -1;
	}

	/**
	 * 指定カッコの終端を検知.
	 * 
	 * @param base
	 *            検索元の情報を設定します.
	 * @param st
	 *            カッコ開始文字列を設定します.
	 * @param ed
	 *            カッコ終了文字列を設定します.
	 * @param off
	 *            対象のオフセット値を設定します.
	 * @return int 検索結果の内容が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final int indexParAndCote(String base, char st, char ed, int off) throws Exception {
		int len = base.length();
		int par = -1;
		int cote = -1;
		int idx = 0;
		int bef = 0;
		boolean yenFlag = false;
		for (int i = off; i < len; i++) {
			char c = base.charAt(i);
			if (par != -1 || cote != -1) {
				if (cote != -1) {
					if (bef != '\\' && c == cote) {
						yenFlag = false;
						cote = -1;
					} else if (c == '\\' && bef == '\\') {
						yenFlag = true;
					} else {
						yenFlag = false;
					}
				} else if (par != -1) {
					if (c == ed) {
						idx--;
						if (idx <= 0) {
							return i;
						}
					} else if (c == st) {
						idx++;
					}
				}
			} else {
				if (c == '\'' || c == '\"') {
					cote = c;
				} else if (c == st) {
					idx = 1;
					par = c;
				} else if (c == ed) {
					return i;
				}
			}
			if (yenFlag) {
				yenFlag = false;
				bef = 0;
			} else {
				bef = c;
			}
		}
		return -1;
	}

	/**
	 * コーテーション内を検知しないIndexOf.
	 * 
	 * @param base
	 *            検索元の情報を設定します.
	 * @param cc
	 *            チェック対象の内容を設定します.
	 * @param off
	 *            対象のオフセット値を設定します.
	 * @return int 検索結果の内容が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final int indexToNotCote(String base, String cc, int off) throws Exception {
		int len = base.length();
		int cote = -1;
		char[] ck = cc.toCharArray();
		int cLen = ck.length;
		char bef = 0;
		boolean yenFlag = false;
		for (int i = off; i < len; i++) {
			char c = base.charAt(i);
			if (cote != -1) {
				if (bef != '\\' && c == cote) {
					yenFlag = false;
					cote = -1;
				} else if (c == '\\' && bef == '\\') {
					yenFlag = true;
				} else {
					yenFlag = false;
				}
			} else {
				if (bef != '\\' && (c == '\'' || c == '\"')) {
					cote = c;
				} else if (c == ck[0]) {
					boolean res = true;
					for (int j = 1; j < cLen; j++) {
						if (i + j >= len || ck[j] != base.charAt(i + j)) {
							res = false;
							break;
						}
					}
					if (res == true) {
						return i;
					}
				}
			}
			if (yenFlag) {
				yenFlag = false;
				bef = 0;
			} else {
				bef = c;
			}
		}
		return -1;
	}

	/**
	 * カッコを検知しないIndexOf.
	 * 
	 * @param base
	 *            検索元の情報を設定します.
	 * @param cc
	 *            チェック対象の内容を設定します.
	 * @param st
	 *            カッコ開始文字列を設定します.
	 * @param ed
	 *            カッコ終了文字列を設定します.
	 * @param off
	 *            対象のオフセット値を設定します.
	 * @return int 検索結果の内容が返されます.
	 * @exception Exception
	 *                例外.
	 */
	public static final int indexToNotPar(String base, String cc, char st, char ed, int off) throws Exception {
		int len = base.length();
		int par = -1;
		char[] ck = cc.toCharArray();
		int cLen = ck.length;
		int idx = 0;
		for (int i = off; i < len; i++) {
			char c = base.charAt(i);
			if (par != -1) {
				if (c == ed) {
					idx--;
					if (idx <= 0) {
						par = -1;
					}
				} else if (c == st) {
					idx++;
				}
			} else {
				if (c == st) {
					idx = 1;
					par = c;
				} else if (c == ck[0]) {
					boolean res = true;
					for (int j = 1; j < cLen; j++) {
						if (i + j >= len || ck[j] != base.charAt(i + j)) {
							res = false;
							break;
						}
					}
					if (res == true) {
						return i;
					}
				}
			}
		}
		return -1;
	}

	/**
	 * 文字列の前後コーテーションを削除.
	 * 
	 * @param str
	 *            変換対象の文字列を設定します.
	 * @return String 変換された結果が返されます.
	 */
	public static final String cutCote(String str) {
		if (str == null || str.length() <= 0) {
			if (str == null) {
				return null;
			} else {
				return "";
			}
		}
		str = str.trim();
		return ((str.startsWith("\'") == true && str.endsWith("\'") == true)
				|| (str.startsWith("\"") == true && str.endsWith("\"") == true)) ? str.substring(1, str.length() - 1)
						: str;
	}

	/**
	 * 指定ワードが存在するかチェック.
	 * 
	 * @param s
	 *            対象のスクリプトを設定します.
	 * @param t
	 *            対象のワードを設定します.
	 * @param off
	 *            スクリプトチェック開始位置を設定します.
	 * @param len
	 *            スクリプト長を設定します.
	 */
	public static final boolean isCode(String s, String t, int off, int len) {
		if (s.charAt(off) == t.charAt(0)) {
			if (t.length() + off > len) {
				return false;
			}
			int ln = t.length();
			int p = 1;
			for (; p < ln && s.charAt(p + off) == t.charAt(p); p++)
				;
			if (ln == p) {
				char c;
				if (p + off == len || !((c = s.charAt(p + off)) >= 'A' && c <= 'z')) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * 指定括弧の終端を取得.
	 * 
	 * @param script
	 *            対象のスクリプトを設定します.
	 * @param target
	 *            対象の括弧開始条件を設定します.
	 * @param off
	 *            対象のオフセット値を設定します.
	 * @param len
	 *            対象のスクリプト長を設定します.
	 * @return int 括弧終端位置が返却されます. [-1]が返却された場合、存在しません.
	 * @exception Exception
	 *                例外.
	 */
	public static final int searchParEnd(String script, String target, int off, int len) throws Exception {
		char endTarget = 0;
		switch (target.charAt(0)) {
		case '(':
			endTarget = ')';
			target = "(";
			break;
		case '[':
			endTarget = ']';
			target = "[";
			break;
		case '{':
			endTarget = '}';
			target = "{";
			break;
		}
		if (endTarget == 0) {
			throw new IOException("括弧条件ではありません");
		}
		off = ParseUtil.indexToNotCote(script, target, off);
		if (off == -1) {
			return -1;
		}
		char c;
		int par1, par2, par3, b;
		int cote = -1;
		par1 = par2 = par3 = 0;
		b = -1;
		for (int i = off; i < len; i++) {
			c = script.charAt(i);
			if (cote == -1) {
				if (b != '\\' && (c == '\'' || c == '\"')) {
					cote = c;
					b = c;
					continue;
				}
			} else {
				if (b != '\\' && cote == c) {
					cote = -1;
				}
				b = c;
				continue;
			}
			switch (c) {
			case '(':
				par1++;
				break;
			case '[':
				par2++;
				break;
			case '{':
				par3++;
				break;
			case ')':
				par1--;
				if (endTarget == c && par1 == 0) {
					return i + 1;
				}
				break;
			case ']':
				par2--;
				if (endTarget == c && par2 == 0) {
					return i + 1;
				}
				break;
			case '}':
				par3--;
				if (endTarget == c && par3 == 0) {
					return i + 1;
				}
				break;
			}
			b = c;
		}
		return -1;
	}

	/**
	 * 指定文字内の改行コードを全て削除.
	 * 
	 * @param s
	 *            対象の文字列を設定します.
	 * @return String 変換された情報が返却されます.
	 */
	public static final String cutEnter(String string) {
		if (string == null || string.length() <= 0) {
			return "";
		}
		StringBuilder buf = new StringBuilder();
		int len = string.length();
		int cote = -1;
		int bef = 0;
		boolean yenFlag = false;
		for (int i = 0; i < len; i++) {
			char c = string.charAt(i);
			if (cote != -1) {
				if (bef != '\\' && c == cote) {
					yenFlag = false;
					cote = -1;
				} else if (c == '\\' && bef == '\\') {
					yenFlag = true;
				} else {
					yenFlag = false;
				}
			} else if (c == '\'' || c == '\"') {
				cote = c;
			}
			if (c == '\r' || c == '\n') {
				if (cote == -1) {
					buf.append(" ");
				}
				continue;
			}
			buf.append(c);
			if (yenFlag) {
				yenFlag = false;
				bef = 0;
			} else {
				bef = c;
			}
		}
		return buf.toString().trim();
	}

}
