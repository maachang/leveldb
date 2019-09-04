package org.maachang.leveldb;

/**
 * KeyValue.
 * 
 * @param <K> key情報.
 * @param <V> value情報.
 */
public class KeyValue<K, V> {
	private K key;
	private V value;

	/**
	 * コンストラクタ.
	 */
	KeyValue() {
	}

	/**
	 * コンストラクタ.
	 * @param k
	 * @param v
	 */
	KeyValue(K k, V v) {
		key = k;
		value = v;
	}

	/**
	 * 情報設定.
	 * @param k
	 * @param v
	 */
	public void set(K k, V v) {
		key = k;
		value = v;
	}

	/**
	 * key情報取得.
	 * @return
	 */
	public K key() {
		return key;
	}

	/**
	 * value情報取得.
	 * @return
	 */
	public V value() {
		return value;
	}
}
