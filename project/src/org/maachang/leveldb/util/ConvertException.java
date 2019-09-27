package org.maachang.leveldb.util;

import org.maachang.leveldb.LeveldbException;

/**
 * 変換系例外.
 */
public class ConvertException extends LeveldbException {
	private static final long serialVersionUID = 1703575082572362938L;

	public ConvertException() {
		super(500);
	}

	public ConvertException(String m) {
		super(500, m);
	}

	public ConvertException(Throwable e) {
		super(500, e);
	}

	public ConvertException(String m, Throwable e) {
		super(500, m, e);
	}
}
