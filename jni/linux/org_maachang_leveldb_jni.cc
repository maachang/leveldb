#include <stdlib.h>
#include <memory.h>

#include "leveldb_src/include/javaLeveldb.h"
#include "snappy_src/snappy_java.h"
#include "snappy_src/lz4_java.h"
#include "org_maachang_leveldb_jni.h"

/** malloc. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_malloc
  (JNIEnv* env, jclass c, jint size) {
    return (jlong)malloc( size ) ;
}

/** realloc. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_realloc
  (JNIEnv* env, jclass c, jlong addr, jint size) {
    return (jlong)realloc( (void*)addr,size ) ;
}

/** free. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_free
  (JNIEnv* env, jclass c, jlong addr) {
    free( (void*)addr ) ;
}

/** memset. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_memset
  (JNIEnv* env, jclass c, jlong addr, jbyte code, jint size ) {
    memset( (void*)addr,code,size ) ;
}

/** memcpy. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_memcpy
  (JNIEnv* env, jclass c, jlong srcAddr, jlong destAddr, jint size ) {
    memcpy( (void*)srcAddr,(void*)destAddr,size ) ;
}

/** memcmp. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_memcmp
  (JNIEnv* env, jclass c, jlong srcAddr, jlong destAddr, jint size ) {
    return memcmp( (const void*)srcAddr,(const void*)destAddr,size ) ;
}

/** putByte. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_putByte
  (JNIEnv* env, jclass c, jlong addr, jbyte value ) {
    *((char*)addr) = value ;
}

/** getByte. **/
JNIEXPORT jbyte JNICALL Java_org_maachang_leveldb_jni_getByte
  (JNIEnv* env, jclass c, jlong addr ) {
    return *((char*)addr) ;
}

/** getBinary. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_getBinary
  (JNIEnv* env, jclass c, jlong addr, jbyteArray bin, jint off, jint len ) {
    env->SetByteArrayRegion( bin,off,len,(jbyte*)addr ) ;
}

/** putBinary. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_putBinary
  (JNIEnv* env, jclass c, jlong addr, jbyteArray bin, jint off, jint len ) {
    env->GetByteArrayRegion( bin,off,len,(jbyte*)addr ) ;
}

/** putChar **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_putChar
  (JNIEnv* env, jclass c, jlong addr, jchar value) {
    *((jchar*)addr) = value ;
}

/** getChar **/
JNIEXPORT jchar JNICALL Java_org_maachang_leveldb_jni_getChar
  (JNIEnv* env, jclass c, jlong addr) {
    return *((jchar*)addr) ;
}

/** putShort **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_putShort
  (JNIEnv* env, jclass c, jlong addr, jshort value) {
    *((jshort*)addr) = value ;
}

/** getShort **/
JNIEXPORT jshort JNICALL Java_org_maachang_leveldb_jni_getShort
  (JNIEnv* env, jclass c, jlong addr) {
    return *((jshort*)addr) ;
}

/** putInt **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_putInt
  (JNIEnv* env, jclass c, jlong addr, jint value) {
    *((jint*)addr) = value ;
}

/** getInt **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_getInt
  (JNIEnv* env, jclass c, jlong addr) {
    return *((jint*)addr) ;
}

/** putLong **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_putLong
  (JNIEnv* env, jclass c, jlong addr, jlong value) {
    *((jlong*)addr) = value ;
}

/** getLong **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_getLong
  (JNIEnv* env, jclass c, jlong addr) {
    return *((jlong*)addr) ;
}

/** binary同一チェック. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_eq
  (JNIEnv* env, jclass c,jlong c1, jlong c2, jint len ) {
    int i ;
    char* v1 ;
    char* v2 ;
    v1 = (char*)(c1) ;
    v2 = (char*)(c2) ;
    for( i = 0 ; i < len ; i ++ ) {
        if( v1[ i ] != v2[ i ] ) {
            return 0 ;
        }
    }
    return 1 ;
}

/** snappy圧縮バッファサイズの計算. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_snappyMaxCompressedLength
  (JNIEnv* env, jclass c, jint oneCompressLength ) {
    
    return (jint)_snappyMaxCompressedLength( (size_t)oneCompressLength ) ;
}

/** snappy圧縮. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_snappyCompress
  (JNIEnv* env, jclass c, jlong src, jint src_len, jlong dst, jintArray dst_len) {
    
    int out;
    _snappyCompress( (char*)src,(int)src_len,(char*)dst,&out ) ;
    env->SetIntArrayRegion( dst_len,0,1,&out ) ;
    return (jint)0 ;
}

/** snappy解凍. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_snappyDecompress
  (JNIEnv* env, jclass c, jlong src, jint src_len, jlong dst, jintArray dst_len) {
    
    size_t out;
    out = _snappyUncompress( (char*)src,src_len,(char*)dst ) ;
    if( out == -1 ) {
        return -1 ;
    }
    env->SetIntArrayRegion( dst_len,0,1,(const jint*)&out ) ;
    return 0 ;
}

/** lz4圧縮バッファサイズの計算. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_lz4MaxCompressedLength
  (JNIEnv* env, jclass c, jint oneCompressLength ) {
    return (jint)_lz4MaxCompressedLength((size_t)oneCompressLength);
}

/** lz4解凍バッファサイズの取得. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_lz4UncompressLength
  (JNIEnv* env, jclass c, jlong src ) {
    size_t out;
    _lz4UncompressLength((char*)src, &out);
    return (jint)out;
}

/** lz4圧縮. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_lz4Compress
  (JNIEnv* env, jclass c, jlong src, jint src_len, jlong dst, jintArray dst_len) {
    int out;
    out = _lz4Compress((char*)src,(char*)dst, (int)src_len );
    env->SetIntArrayRegion(dst_len, 0, 1, &out);
    return (jint)0 ;
}

/** lz4解凍. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_lz4Decompress
  (JNIEnv* env, jclass c, jlong src, jint src_len, jlong dst, jintArray dst_len) {
    size_t out = _lz4Uncompress((char*)src, src_len,(char*)dst) ;
    if( out == -1 ) {
        return -1 ;
    }
    env->SetIntArrayRegion(dst_len, 0, 1, (const jint*)&out) ;
    return 0 ;
}

/** Leveldb破棄. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1destroy
  (JNIEnv * env , jclass c , jlong name, jint type, jint write_buffer_size,
  jint max_open_files,jint block_size,jint block_restart_interval ) {
    
    java_leveldb_destroy( name,type,write_buffer_size,max_open_files,block_size,block_restart_interval ) ;
}

/** Leveldb修復. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1repair
  (JNIEnv * env , jclass c , jlong name, jint type, jint write_buffer_size,
  jint max_open_files,jint block_size,jint block_restart_interval ) {
    
    java_leveldb_repair( name,type,write_buffer_size,max_open_files,block_size,block_restart_interval ) ;
}

/** Leveldbオープン. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1open
  (JNIEnv * env , jclass c , jlong name, jint type, jint write_buffer_size,
  jint max_open_files,jint block_size,jint block_restart_interval,jint block_cache ) {
    
    return java_leveldb_open( name,type,write_buffer_size,
        max_open_files,block_size,block_restart_interval,block_cache ) ;
}

/** Leveldbクローズ. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1close
  (JNIEnv * env , jclass c , jlong db ) {
    
    java_leveldb_close( db ) ;
}

/** Leveldb要素セット. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1put
  (JNIEnv * env , jclass c , jlong db, jlong key, jint kLen, jlong value , jint vLen ) {
    
    return java_leveldb_put( db,key,kLen,value,vLen ) ;
}

/** Leveldb要素取得. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1get
  (JNIEnv * env , jclass c , jlong db , jlong key, jint len,
  jlongArray buf, jint bufLen) {
    
    return java_leveldb_get( env,db,key,len,buf,bufLen ) ;
}

/** Leveldb要素削除. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1remove
  (JNIEnv * env , jclass c , jlong db, jlong key, jint len ) {
    
    return java_leveldb_remove( db,key,len ) ;
}

/** Leveldb状態取得. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1property
  (JNIEnv * env , jclass c , jlong db , jlong cmd, jint len,
  jlongArray buf, jint bufLen) {
    
    return java_leveldb_property( env,db,cmd,len,buf,bufLen ) ;
}

/** Leveldbのvacuum処理. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1vacuum
  (JNIEnv * env , jclass c , jlong db , jlong start,jint startLen, jlong end, jint endLen ) {
    
    java_leveldb_vacuum( db,start,startLen,end,endLen ) ;
}


/** Iterator作成. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1iterator
  (JNIEnv * env , jclass c , jlong db ) {
    
    return java_leveldb_iterator( db ) ;
}

/** Iteratorクローズ. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1delete
  (JNIEnv * env , jclass c , jlong itr ) {
    
    java_leveldb_itr_delete( itr ) ;
}

/** Iterator先頭に移動. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1first
  (JNIEnv * env , jclass c , jlong itr ) {
    
    java_leveldb_itr_first( itr ) ;
}

/** Iterator最後に移動. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1last
  (JNIEnv * env , jclass c , jlong itr ) {
    
    java_leveldb_itr_last( itr ) ;
}

/** Iteratorシーク位置に移動. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1seek
  (JNIEnv * env , jclass c , jlong itr, jlong key, jint len ) {
    
    java_leveldb_itr_seek( itr,key,len ) ;
}

/** Iterator現在位置カーソルの情報存在確認. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1valid
  (JNIEnv * env , jclass c , jlong itr ) {

    return java_leveldb_itr_valid( itr ) ;
}

/** Iteratorカーソルを次に移動. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1next
  (JNIEnv * env , jclass c , jlong itr ) {
    
    java_leveldb_itr_next( itr ) ;
}

/** Iteratorカーソルを前に移動. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1before
  (JNIEnv * env , jclass c , jlong itr ) {
    
    java_leveldb_itr_before( itr ) ;
}

/** Iteratorカーソル位置のKeyを取得. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1key
  (JNIEnv * env , jclass c , jlong itr , jlongArray out, jint bufLen ) {
    
    return java_leveldb_itr_key( env,itr,out,bufLen ) ;
}

/** Iteratorカーソル位置のValueを取得. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1value
  (JNIEnv * env , jclass c , jlong itr, jlongArray out, jint bufLen ) {
    
    return java_leveldb_itr_value( env,itr,out,bufLen ) ;
}



/** WriteBatch情報を生成. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1create
  (JNIEnv * env , jclass c ) {
    
    return java_leveldb_wb_create() ;
}

/** WriteBatch情報をサイズ指定で生成. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1create_1by_1size
  (JNIEnv * env , jclass c, jint size ) {
    return java_leveldb_wb_create_by_size( size ) ;
}

/** WriteBatch情報を削除. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1destroy
  (JNIEnv * env , jclass c, jlong wb ) {
    
    java_leveldb_wb_destroy( wb ) ;
}

/** WriteBatch情報をサイズ指定してクリア. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1clear_1by_1size
  (JNIEnv * env , jclass c, jlong wb, jint size ) {
    
    java_leveldb_wb_clear_by_size( wb,size ) ;
}

/** WriteBatch情報をクリア. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1clear
  (JNIEnv * env , jclass c, jlong wb ) {
    
    java_leveldb_wb_clear( wb ) ;
}

/** WriteBatch情報に追加指定. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1put
  (JNIEnv * env , jclass c, jlong wb, jlong key, jint keyLen, jlong value, jint valueLen ) {
    
    java_leveldb_wb_put( wb,key,keyLen,value,valueLen ) ;
}

/** WriteBatch情報に削除指定. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1remove
  (JNIEnv * env , jclass c, jlong wb, jlong key, jint len ) {
    
    java_leveldb_wb_remove( wb,key,len ) ;
}

/** WriteBatch情報の内容を取得. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1values
  (JNIEnv * env , jclass c, jlong wb ) {
    
    return java_leveldb_wb_values( wb ) ;
}

/** WriteBatch情報の内容サイズを取得. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1values_1size
  (JNIEnv * env , jclass c, jlong wb ) {
    
    return java_leveldb_wb_values_size( wb ) ;
}

/** WriteBatch情報をDBに反映. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1flush
  (JNIEnv * env , jclass c, jlong db , jlong wb ) {
    
    return java_leveldb_wb_flush( db,wb ) ;
}

/** SnapShotを生成. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1ss_1create
  (JNIEnv * env , jclass c, jlong db ) {
    
    return java_leveldb_createSnapShot( db ) ;
}

/** SnapShotを破棄. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1ss_1destroy
  (JNIEnv * env , jclass c, jlong db, jlong ss ) {
    
    java_leveldb_releaseSnapShot( db,ss ) ;
}

/** SnapShot用Iteratorを生成. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1ss_1iterator
  (JNIEnv * env , jclass c, jlong db, jlong ss ) {
    
    return java_leveldb_getSnapShotIterator( db,ss ) ;
}

