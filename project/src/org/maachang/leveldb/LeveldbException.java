package org.maachang.leveldb;

/**
 * Leveldb例外.
 */
public class LeveldbException extends RuntimeException {
	private static final long serialVersionUID = 8207343446412052558L;
	protected int status;

	public LeveldbException(int status) {
		super();
		this.status = status;
	}

	public LeveldbException(int status, String message) {
		super(message);
		this.status = status;
	}

	public LeveldbException(int status, Throwable e) {
		super(e);
		this.status = status;
	}

	public LeveldbException(int status, String message, Throwable e) {
		super(message, e);
		this.status = status;
	}

	public LeveldbException() {
		this(500);
	}

	public LeveldbException(String m) {
		this(500, m);
	}

	public LeveldbException(Throwable e) {
		this(500, e);
	}

	public LeveldbException(String m, Throwable e) {
		this(500, m, e);
	}

	public int getStatus() {
		return status;
	}
}
