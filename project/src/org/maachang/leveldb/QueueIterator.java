package org.maachang.leveldb;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Queue用Iterator.
 */
public class QueueIterator implements Iterator<KeyValue<String, Object>> {
	private KeyValue<String, Object> keyValue = new KeyValue<String, Object>();
	private LeveldbIterator itr = null;
	private Queue queue = null;
	
	QueueIterator(Queue q, LeveldbIterator i, byte[] key) {
		if (key != null) {
			JniBuffer buf = null;
			try {
				buf = LevelBuffer.key(LevelOption.TYPE_FREE, key);
				i.seek(buf);
			} catch (Exception e) {
				if (i != null) {
					i.close();
				}
				throw new LeveldbException(e);
			} finally {
				LevelBuffer.clearBuffer(buf, null);
			}
		}
		queue = q;
		itr = i;
	}

	// ファイナライズ.
	protected void finalize() throws Exception {
		close();
	}

	/**
	 * クローズ.
	 * 利用後はクローズ処理を行います.
	 */
	public void close() {
		LeveldbIterator i = itr; itr = null;
		if (i != null) {
			i.close();
		}
		keyValue = null;
	}

	/**
	 * 次の情報が存在するかチェック.
	 * @return
	 */
	@Override
	public boolean hasNext() {
		if (queue.isClose() || itr == null || !itr.valid()) {
			close();
			return false;
		}
		return true;
	}

	/**
	 * 次の情報を取得.
	 * @return
	 */
	@Override
	public KeyValue<String, Object> next() {
		if (queue.isClose() || itr == null || !itr.valid()) {
			close();
			throw new NoSuchElementException();
		}
		JniBuffer keyBuf = null;
		JniBuffer valBuf = null;
		try {
			keyBuf = LevelBuffer.key();
			valBuf = LevelBuffer.value();
			itr.key(keyBuf);
			itr.value(valBuf);
			itr.next();
			if (!itr.valid()) {
				close();
			}
			keyValue.set(Time12SequenceId.toString(keyBuf.getBinary()),
				LevelValues.decode(valBuf));
			LevelBuffer.clearBuffer(keyBuf, valBuf);
			keyBuf = null; valBuf = null;
			return keyValue;
		} catch (LeveldbException le) {
			throw le;
		} catch (Exception e) {
			throw new LeveldbException(e);
		} finally {
			LevelBuffer.clearBuffer(keyBuf, valBuf);
		}
	}
}
