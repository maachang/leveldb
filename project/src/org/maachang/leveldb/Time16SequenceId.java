package org.maachang.leveldb;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Time 16 byte(128bit) シーケンスID発行処理.
 */
public class Time16SequenceId {
	private final AtomicInteger nowId = new AtomicInteger(0);
	private final AtomicLong nowTime = new AtomicLong(-1L);
	private int machineId = 0;

	/**
	 * コンストラクタ.
	 * 
	 * @param id
	 *			対象のマシンIDを設定します.
	 */
	public Time16SequenceId(int id) {
		machineId = id;
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param id
	 *			対象のマシンIDを設定します.
	 * @param lastTime
	 *			設定した最終時間を設定します.
	 * @param lastId
	 *			設定した最終IDを設定します.
	 */
	public Time16SequenceId(int id, long lastTime, int lastId) {
		nowTime.set(lastTime);
		machineId = id;
		nowId.set(lastId);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param binary
	 *			対象のバイナリを設定します.
	 */
	public Time16SequenceId(byte[] binary) {
		set(binary);
	}

	/**
	 * コンストラクタ.
	 * 
	 * @param uuid
	 *			対象のUUIDを設定します.
	 */
	public Time16SequenceId(String uuid) {
		set(uuid);
	}

	/**
	 * シーケンスIDを発行.
	 * 
	 * @param buf
	 *			対象のバッファを設定します.
	 */
	public final void next(byte[] out) {
		next(out, false);
	}

	/**
	 * シーケンスIDを発行.
	 * 
	 * @return byte[] シーケンスIDが発行されます.
	 */
	public final byte[] next() {
		final byte[] ret = new byte[16];
		next(ret);
		return ret;
	}

	/**
	 * シーケンスIDを発行.
	 * 
	 * @return String シーケンスIDが発行されます.
	 */
	public final String nextUUID() {
		return next(null, true);
	}

	/**
	 * 現在発行したシーケンスIDを再取得.
	 * 
	 * @param buf
	 *			対象のバッファを設定します.
	 */
	public final void get(byte[] buf) {
		createId(buf, machineId, nowTime.get(), nowId.get());
	}

	/**
	 * 現在発行したシーケンスIDを再取得.
	 * 
	 * @return byte[] シーケンスIDが発行されます.
	 */
	public final byte[] get() {
		final byte[] ret = new byte[16];
		get(ret);
		return ret;
	}

	/**
	 * 現在発行したシーケンスIDを再取得.
	 * 
	 * @return String シーケンスIDが発行されます.
	 */
	public final String uuid() {
		return createId(machineId, nowTime.get(), nowId.get());
	}

	/**
	 * シーケンスIDを設定.
	 * 
	 * @param binary
	 *			対象のバイナリを設定します.
	 */
	public final void set(byte[] binary) {
		setBinary(binary);
	}

	/**
	 * シーケンスIDを設定.
	 * 
	 * @param uuid
	 *			対象のUUIDを設定します.
	 */
	public final void set(String uuid) {
		setUUID(uuid);
	}

	/**
	 * マシンIDを取得.
	 * 
	 * @return int 設定されているマシンIDが返却されます.
	 */
	public final int getMachineId() {
		return (int) machineId;
	}

	// ID生成.
	private final String next(byte[] out, boolean mode) {
		int id;
		long beforeTime, time;
		while (true) {
			id = nowId.get();
			beforeTime = nowTime.get();
			time = System.currentTimeMillis();

			// システム時間が変更された場合.
			if (time != beforeTime) {
				if (id < Integer.MAX_VALUE
						&& nowTime.compareAndSet(beforeTime, time)
						&& nowId.compareAndSet(id, 0)) {
					if (mode) {
						return createId(machineId, time, 0);
					}
					createId(out, machineId, time, 0);
					return null;
				}
			}
			// シーケンスIDインクリメント.
			else if (nowId.compareAndSet(id, id + 1)) {
				if (mode) {
					return createId(machineId, beforeTime, id + 1);
				}
				createId(out, machineId, beforeTime, id + 1);
				return null;
			}
		}
	}

	// バイナリ変換.
	private static final void createId(final byte[] out, final int machineId, final long time, final int seqId) {
		out[0] = (byte) ((time & 0xff00000000000000L) >> 56L);
		out[1] = (byte) ((time & 0x00ff000000000000L) >> 48L);
		out[2] = (byte) ((time & 0x0000ff0000000000L) >> 40L);
		out[3] = (byte) ((time & 0x000000ff00000000L) >> 32L);
		out[4] = (byte) ((time & 0x00000000ff000000L) >> 24L);
		out[5] = (byte) ((time & 0x0000000000ff0000L) >> 16L);
		out[6] = (byte) ((time & 0x000000000000ff00L) >> 8L);
		out[7] = (byte) ((time & 0x00000000000000ffL) >> 0L);

		out[8] = (byte) ((seqId & 0xff000000) >> 24);
		out[9] = (byte) ((seqId & 0x00ff0000) >> 16);
		out[10] = (byte) ((seqId & 0x0000ff00) >> 8);
		out[11] = (byte) ((seqId & 0x000000ff) >> 0);

		out[12] = (byte) ((machineId & 0xff000000) >> 24);
		out[13] = (byte) ((machineId & 0x00ff0000) >> 16);
		out[14] = (byte) ((machineId & 0x0000ff00) >> 8);
		out[15] = (byte) ((machineId & 0x000000ff) >> 0);
	}

	// byte１６進数変換 + ゼロサプレス.
	private static final String zero2(final int no) {
		return no >= 16 ? Integer.toHexString(no) : "0" + Integer.toHexString(no);
	}

	// UUID変換.
	private static final String createId(final int machineId, final long time, final int seqId) {
		return new StringBuilder()
			.append(zero2((int) ((time & 0xff00000000000000L) >> 56L)))
			.append(zero2((int) ((time & 0x00ff000000000000L) >> 48L)))
			.append(zero2((int) ((time & 0x0000ff0000000000L) >> 40L)))
			.append(zero2((int) ((time & 0x000000ff00000000L) >> 32L)))
			.append("-")
			.append(zero2((int) ((time & 0x00000000ff000000L) >> 24L)))
			.append(zero2((int) ((time & 0x0000000000ff0000L) >> 16L)))
			.append("-")
			.append(zero2((int) ((time & 0x000000000000ff00L) >> 8L)))
			.append(zero2((int) ((time & 0x00000000000000ffL) >> 0L)))
			.append("-")
			.append(zero2((int) ((seqId & 0xff000000) >> 24)))
			.append(zero2((int) ((seqId & 0x00ff0000) >> 16)))
			.append("-")
			.append(zero2((int) ((seqId & 0x0000ff00) >> 8)))
			.append(zero2((int) ((seqId & 0x000000ff) >> 0)))
			.append(zero2((int) ((machineId & 0xff000000) >> 24)))
			.append(zero2((int) ((machineId & 0x00ff0000) >> 16)))
			.append(zero2((int) ((machineId & 0x0000ff00) >> 8)))
			.append(zero2((int) ((machineId & 0x000000ff) >> 0)))
			.toString();
	}

	// バイナリから、データ変換.
	private final void setBinary(final byte[] value) {
		nowId.set(uuidToSequenceId(value));
		nowTime.set(uuidToTime(value));
		machineId = uuidToMachineId(value);
	}

	// UUIDから、データ変換.
	private final void setUUID(final String uuid) {
		setBinary(getBytes(uuid));
	}

	/**
	 * UUID文字列をバイナリに変換. UUIDが正しいかチェックしたい場合にも利用できます.
	 * 
	 * @param uuid
	 *			対象のUUIDを設定します.
	 * @return byte[] 戻り値が存在しない場合(null)は、変換に失敗しました.
	 */
	public static final byte[] getBytes(final String uuid) {
		if (uuid.length() != 36) {
			return null;
		}
		try {
			return new byte[] {
				toByte(uuid, 0),
				toByte(uuid, 2),
				toByte(uuid, 4),
				toByte(uuid, 6),
				toByte(uuid, 9), // -
				toByte(uuid, 11),
				toByte(uuid, 14), // -
				toByte(uuid, 16),
				toByte(uuid, 19),// -
				toByte(uuid, 21),
				toByte(uuid, 24),// -
				toByte(uuid, 26),
				toByte(uuid, 28),
				toByte(uuid, 30),
				toByte(uuid, 32),
				toByte(uuid, 34) };
		} catch (NumberFormatException ne) {
			return null;
		}
	}

	/**
	 * UUID(binary)から、時間を取得.
	 * 
	 * @param value
	 * @return
	 */
	public static final long uuidToTime(final byte[] value) {
		return (((long) value[0] & 0x00000000000000ffL) << 56L)
			| (((long) value[1] & 0x00000000000000ffL) << 48L)
			| (((long) value[2] & 0x00000000000000ffL) << 40L)
			| (((long) value[3] & 0x00000000000000ffL) << 32L)
			| (((long) value[4] & 0x00000000000000ffL) << 24L)
			| (((long) value[5] & 0x00000000000000ffL) << 16L)
			| (((long) value[6] & 0x00000000000000ffL) << 8L)
			| (((long) value[7] & 0x00000000000000ffL) << 0L);
	}

	/**
	 * UUID(binary)からシーケンスIDを取得.
	 * 
	 * @param value
	 * @return
	 */
	public static final int uuidToSequenceId(final byte[] value) {
		return (((int) value[8] & 0x000000ff) << 24)
			| (((int) value[9] & 0x000000ff) << 16)
			| (((int) value[10] & 0x000000ff) << 8)
			| (((int) value[11] & 0x000000ff) << 0);
	}

	/**
	 * UUID(binary)からマシンIDを取得.
	 * 
	 * @param value
	 * @return
	 */
	public static final int uuidToMachineId(final byte[] value) {
		return (((int) value[12] & 0x000000ff) << 24)
			| (((int) value[13] & 0x000000ff) << 16)
			| (((int) value[14] & 0x000000ff) << 8)
			| (((int) value[15] & 0x000000ff) << 0);
	}

	// 指定16進数の文字列をバイナリに変換.
	private static final byte toByte(final String value, final int off) {
		int ret = 0;
		for(int i = 0; i < 2; i ++) {
			switch(value.charAt(off + (1 - i))) {
			case '0': break;
			case '1': ret |= (1<<(i<<2)); break;
			case '2': ret |= (2<<(i<<2)); break;
			case '3': ret |= (3<<(i<<2)); break;
			case '4': ret |= (4<<(i<<2)); break;
			case '5': ret |= (5<<(i<<2)); break;
			case '6': ret |= (6<<(i<<2)); break;
			case '7': ret |= (7<<(i<<2)); break;
			case '8': ret |= (8<<(i<<2)); break;
			case '9': ret |= (9<<(i<<2)); break;
			case 'a': ret |= (10<<(i<<2)); break;
			case 'A': ret |= (10<<(i<<2)); break;
			case 'b': ret |= (11<<(i<<2)); break;
			case 'B': ret |= (11<<(i<<2)); break;
			case 'c': ret |= (12<<(i<<2)); break;
			case 'C': ret |= (12<<(i<<2)); break;
			case 'd': ret |= (13<<(i<<2)); break;
			case 'D': ret |= (13<<(i<<2)); break;
			case 'e': ret |= (14<<(i<<2)); break;
			case 'E': ret |= (14<<(i<<2)); break;
			case 'f': ret |= (15<<(i<<2)); break;
			case 'F': ret |= (15<<(i<<2)); break;
			}
		}
		return (byte)ret;
	}
}
