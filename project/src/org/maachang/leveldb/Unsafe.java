package org.maachang.leveldb;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * sun.misc.Unsafe.
 */
public final class Unsafe {

	public static final Object unsafe;
	public static final boolean UNSAFE_MODE;
	public static final boolean BIG_ENDIAN;

	protected static final Method directByteBufferAddress;

	// unsafeが使えなくなっても [UNSAFE_MODE] 判別して処理するので、
	// 内部所有のAPIが使えなくなっても無問題(だと思う).
	static {

		// //////////////
		// unsafe取得.
		// //////////////
		Object u = null;
		try {
			Field f = Class.forName("sun.misc.Unsafe")
				.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			u = f.get(null);
		} catch (Throwable e) {
			u = null;
		}
		unsafe = u;
		UNSAFE_MODE = (u != null);
		u = null;

		// ///////////////////
		// エンディアン取得.
		// ///////////////////
		boolean md = false;

		if (unsafe != null) {
			sun.misc.Unsafe uo = (sun.misc.Unsafe) unsafe;
			long a = uo.allocateMemory(8);
			try {
				uo.putLong(a, 0x0102030405060708L);
				byte b = uo.getByte(a);
				switch (b) {
				case 0x01:
					md = true;
					break;
				case 0x08:
					md = false;
					break;
				default:
					md = false;
				}
			} finally {
				uo.freeMemory(a);
			}
		} else {
			long a = jni.malloc(8);
			try {
				jni.putLong(a, 0x0102030405060708L);
				byte b = jni.getByte(a);
				switch (b) {
				case 0x01:
					md = true;
					break;
				case 0x08:
					md = false;
					break;
				default:
					md = false;
				}
			} finally {
				jni.free(a);
			}
		}
		BIG_ENDIAN = md;

		// ///////////////////////////////////////////////
		// DirectBufferのNativeアドレスReflectionを取得.
		// ///////////////////////////////////////////////
		Method dbAddrMethod = null;
		try {
			dbAddrMethod = Class.forName("java.nio.DirectByteBuffer")
				.getDeclaredMethod("address");
			dbAddrMethod.setAccessible(true);
		} catch (Exception e) {
			dbAddrMethod = null;
		} finally {
		}
		directByteBufferAddress = dbAddrMethod;
	}
	
	/**
	 * Unsafe例外.
	 */
	public static final class UnsafeException extends RuntimeException {
		private static final long serialVersionUID = -3978791872899460686L;
		UnsafeException() {
			super();
		}
		UnsafeException(String message) {
			super(message);
		}
		UnsafeException(Throwable e) {
			super((e instanceof InvocationTargetException) ? ((InvocationTargetException)e).getCause() : e);
		}
	}
	
	/**
	 * sun.misc.Unsafeオブジェクトを取得.
	 * 
	 * @return sun.misc.Unsafe オブジェクトが返されます.
	 */
	public static final sun.misc.Unsafe get() {
		if (unsafe != null) {
			return (sun.misc.Unsafe) unsafe;
		}
		return null;
	}
	
	/**
	 * DirectByteBufferのNativeアドレスを取得.
	 * 
	 * @param buf
	 *            対象のByteBufferオブジェクトを設定します.
	 * @return long Nativeアドレスが返されます.
	 */
	public static final long getDirectByteBufferAddress(final ByteBuffer buf) {
		if (directByteBufferAddress == null || buf == null || !buf.isDirect()) {
			if (directByteBufferAddress == null) {
				throw new IllegalStateException("This method is not supported.");
			} else if (buf == null) {
				throw new IllegalArgumentException("Argument is invalid.");
			}
			throw new IllegalArgumentException("The specified ByteBuffer is not a DirectByteBuffer.");
		}
		try {
			return (Long) directByteBufferAddress.invoke(buf);
		} catch(Exception e) {
			throw new UnsafeException(e);
		}
	}

	/**
	 * shortSwap.
	 */
	public static final short swap(final short x) {
		return (short) ((x << 8) | ((x >> 8) & 0x000000ff));
	}

	/**
	 * charSwap.
	 */
	public static final char swap(final char x) {
		return (char) ((x << 8) | ((x >> 8) & 0x000000ff));
	}

	/**
	 * intSwap.
	 */
	public static final int swap(final int x) {
		return (((x & 0x000000ff) << 24) | ((x & 0x0000ff00) << 8) | ((x & 0x00ff0000) >> 8)
				| (((x & 0xff000000) >> 24) & 0x000000ff));
	}

	/**
	 * longSwap.
	 */
	public static final long swap(final long x) {
		return (((x & 0x00000000000000ffL) << 56L) | ((x & 0x000000000000ff00L) << 40L)
				| ((x & 0x0000000000ff0000L) << 24L) | ((x & 0x00000000ff000000L) << 8L)
				| ((x & 0x000000ff00000000L) >> 8L) | ((x & 0x0000ff0000000000L) >> 24L)
				| ((x & 0x00ff000000000000L) >> 40L) | (((x & 0xff00000000000000L) >> 56L) & 0x00000000000000ffL));
	}
}
