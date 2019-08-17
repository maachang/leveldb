#ifndef _JAVA_LEVEL_DB_INCLUDE_H_
#define _JAVA_LEVEL_DB_INCLUDE_H_

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Leveldb破棄. **/
void java_leveldb_destroy( jlong name,jint type,jint write_buffer_size,
    jint max_open_files,jint block_size,jint block_restart_interval ) ;

/** Leveldb調整. **/
void java_leveldb_repair( jlong name,jint type,jint write_buffer_size,
    jint max_open_files,jint block_size,jint block_restart_interval ) ;

/** Leveldbオープン. **/
jlong java_leveldb_open( jlong name,jint type,jint write_buffer_size,
    jint max_open_files,jint block_size,jint block_restart_interval,jint block_cache ) ;

/** Leveldbクローズ. **/
void java_leveldb_close( jlong db ) ;

/** Leveldb要素セット. **/
jint java_leveldb_put( jlong db, jlong key, jint kLen, jlong value , jint vlen ) ;

/** Leveldb要素取得. **/
jint java_leveldb_get( JNIEnv* env, jlong db , jlong key, jint len, jlongArray buf, jint bufLen ) ;

/** Leveldb要素削除. **/
jint java_leveldb_remove( jlong db, jlong key, jint len ) ;

/** Leveldb状態取得. **/
jint java_leveldb_property( JNIEnv* env, jlong db , jlong cmd, jint len, jlongArray buf, jint bufLen ) ;

/** vacuum的な処理. **/
void java_leveldb_vacuum( jlong db, jlong start,jint startLen, jlong end, jint endLen ) ;

/** Iterator作成. **/
jlong java_leveldb_iterator( jlong db ) ;

/** Iteratorクローズ. **/
void java_leveldb_itr_delete( jlong itr ) ;

/** Iterator先頭に移動. **/
void java_leveldb_itr_first( jlong itr ) ;

/** Iterator最後に移動. **/
void java_leveldb_itr_last( jlong itr ) ;

/** Iteratorシーク位置に移動. **/
void java_leveldb_itr_seek( jlong itr, jlong key,jint len ) ;

/** Iterator現在位置カーソルの情報存在確認. **/
jint java_leveldb_itr_valid( jlong itr ) ;

/** Iteratorカーソルを次に移動. **/
void java_leveldb_itr_next( jlong itr ) ;

/** Iteratorカーソルを前に移動. **/
void java_leveldb_itr_before( jlong itr ) ;

/** Iteratorカーソル位置のKeyを取得. **/
jint java_leveldb_itr_key( JNIEnv* env, jlong itr , jlongArray out, jint bufLen ) ;

/** Iteratorカーソル位置のValueを取得. **/
jint java_leveldb_itr_value( JNIEnv* env, jlong itr, jlongArray out, jint bufLen ) ;


/** WriteBatchを生成. **/
jlong java_leveldb_wb_create() ;

/** WriteBatchをメモリサイズを設定して生成. **/
jlong java_leveldb_wb_create_by_size( jint size ) ;

/** WriteBatchを破棄. **/
void java_leveldb_wb_destroy( jlong wb ) ;

/** WriteBatchをメモリサイズを設定してクリア. **/
void java_leveldb_wb_clear_by_size( jlong wb,jint size ) ;

/** WriteBatchをクリア. **/
void java_leveldb_wb_clear( jlong wb ) ;

/** WriteBatchに情報をセット. **/
void java_leveldb_wb_put( jlong wb, jlong key, jint kLen, jlong value , jint vLen ) ;

/** WriteBatchに情報を削除. **/
void java_leveldb_wb_remove( jlong wb, jlong key, jint len ) ;

/** WriteBatchの内容を取得. **/
jlong java_leveldb_wb_values( jlong wb ) ;

/** WriteBatchの内容長を取得. **/
jint java_leveldb_wb_values_size( jlong wb ) ;

/** WriteBatchをDBに反映. **/
jint java_leveldb_wb_flush( jlong db,jlong wb ) ;


/** SnapShotを生成. **/
jlong java_leveldb_createSnapShot( jlong db ) ;

/** SnapShort用のIteratorを生成. **/
jlong java_leveldb_getSnapShotIterator( jlong db,jlong snapShot ) ;

/** 取得したSnapShortを解放. **/
void java_leveldb_releaseSnapShot( jlong db,jlong snapShot ) ;


#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif /** _JAVA_LEVEL_DB_INCLUDE_H_ **/
