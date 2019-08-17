package org.maachang.leveldb;

/**
 * Leveldb例外.
 */
public class LeveldbException extends RuntimeException {
	private static final long serialVersionUID = 8207343446412052558L;

	public LeveldbException() {
		super();
	}

	public LeveldbException(String m) {
		super(m);
	}

	public LeveldbException(Throwable e) {
		super(e);
	}

	public LeveldbException(String m, Throwable e) {
		super(m, e);
	}
}
