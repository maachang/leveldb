package org.maachang.leveldb;

/**
 * Queue情報.
 */
public interface Queue {
	/**
	 * クローズ処理.
	 */
	public void close();
	
	/**
	 * クローズしているかチェック.
	 * 
	 * @return boolean [true]の場合、クローズしています.
	 */
	public boolean isClose();
	
	/**
	 * Leveldbオブジェクトを取得.
	 * 
	 * @return Leveldb Leveldbオブジェクトが返却されます.
	 */
	public Leveldb getLeveldb();
	
	/**
	 * Levedbオープンオプションを取得.
	 * 
	 * @return LevelOption オプション情報が返却されます.
	 */
	public LevelOption getOption();
	
	/**
	 * 最後に追加.
	 * 
	 * @param o
	 * @return
	 */
	public byte[] add(Object o);
	
	/**
	 * 先頭の情報を取得して削除.
	 * 
	 * @param out
	 *            key情報を取得する場合に設定します.
	 * @return
	 */
	public Object get();
	
	/**
	 * 先頭の情報を取得して削除.
	 * 
	 * @param out
	 *            key情報を取得する場合に設定します.
	 * @return
	 */
	public Object get(String[] out);
	
	/**
	 * 情報が空かチェック.
	 * 
	 * @return boolean [true]の場合、空です.
	 */
	public boolean isEmpty();
	
	/**
	 * iteratorを取得.
	 * 
	 * @param key
	 * @return
	 */
	public QueueIterator iterator();

	/**
	 * iteratorを取得.
	 * 
	 * @param time
	 *            開始位置のミリ秒からのunix時間を設定します.
	 * @return
	 */
	public QueueIterator iterator(long time);

	/**
	 * iteratorを取得.
	 * 
	 * @param key
	 *            開始位置のキー情報を設定します.
	 * @return
	 */
	public QueueIterator iterator(String key);

	/**
	 * iteratorを取得.
	 * 
	 * @param key
	 *            開始位置のキー情報を設定します.
	 * @return
	 */
	public QueueIterator iterator(byte[] key);
}
