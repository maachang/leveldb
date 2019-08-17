#include <stdlib.h>
#include <memory.h>

#include "leveldb_src/include/javaLeveldb.h"
#include "snappy_src/snappy_java.h"
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
    (*env)->SetByteArrayRegion( env,bin,off,len,(char*)addr ) ;
}

/** putBinary. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_putBinary
  (JNIEnv* env, jclass c, jlong addr, jbyteArray bin, jint off, jint len ) {
    (*env)->GetByteArrayRegion( env,bin,off,len,(char*)addr ) ;
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

/** binary����`�F�b�N. **/
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

/** snappy���k�o�b�t�@�T�C�Y�̌v�Z. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_snappyMaxCompressedLength
  (JNIEnv* env, jclass c, jint oneCompressLength ) {
    
    return (jint)_snappyMaxCompressedLength( (size_t)oneCompressLength ) ;
}

/** snappy���k. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_snappyCompress
  (JNIEnv* env, jclass c, jlong src, jint src_len, jlong dst, jintArray dst_len) {
    
    int out;
    _snappyCompress( (char*)src,(int)src_len,(char*)dst,&out ) ;
    (*env)->SetIntArrayRegion( env,dst_len,0,1,&out ) ;
    return (jint)0 ;
}

/** snappy��. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_snappyDecompress
  (JNIEnv* env, jclass c, jlong src, jint src_len, jlong dst, jintArray dst_len) {
    
    size_t out;
    out = _snappyUncompress( (char*)src,src_len,(char*)dst ) ;
    if( out == -1 ) {
        return -1 ;
    }
    (*env)->SetIntArrayRegion( env,dst_len,0,1,(const jint*)&out ) ;
    return 0 ;
}

/** Leveldb�j��. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1destroy
  (JNIEnv * env , jclass c , jlong name, jint type, jint write_buffer_size,
  jint max_open_files,jint block_size,jint block_restart_interval ) {
    
    java_leveldb_destroy( name,type,write_buffer_size,max_open_files,block_size,block_restart_interval ) ;
}

/** Leveldb�C��. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1repair
  (JNIEnv * env , jclass c , jlong name, jint type, jint write_buffer_size,
  jint max_open_files,jint block_size,jint block_restart_interval ) {
    
    java_leveldb_repair( name,type,write_buffer_size,max_open_files,block_size,block_restart_interval ) ;
}

/** Leveldb�I�[�v��. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1open
  (JNIEnv * env , jclass c , jlong name, jint type, jint write_buffer_size,
  jint max_open_files,jint block_size,jint block_restart_interval,jint block_cache ) {
    
    return java_leveldb_open( name,type,write_buffer_size,max_open_files,
        block_size,block_restart_interval,block_cache ) ;
}

/** Leveldb�N���[�Y. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1close
  (JNIEnv * env , jclass c , jlong db ) {
    
    java_leveldb_close( db ) ;
}

/** Leveldb�v�f�Z�b�g. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1put
  (JNIEnv * env , jclass c , jlong db, jlong key, jint kLen, jlong value , jint vLen ) {
    
    return java_leveldb_put( db,key,kLen,value,vLen ) ;
}

/** Leveldb�v�f�擾. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1get
  (JNIEnv * env , jclass c , jlong db , jlong key, jint len,
  jlongArray buf, jint bufLen) {
    
    return java_leveldb_get( env,db,key,len,buf,bufLen ) ;
}

/** Leveldb�v�f�폜. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1remove
  (JNIEnv * env , jclass c , jlong db, jlong key, jint len ) {
    
    return java_leveldb_remove( db,key,len ) ;
}

/** Leveldb��Ԏ擾. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1property
  (JNIEnv * env , jclass c , jlong db , jlong cmd, jint len,
  jlongArray buf, jint bufLen) {
    
    return java_leveldb_property( env,db,cmd,len,buf,bufLen ) ;
}

/** Leveldb��vacuum����. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1vacuum
  (JNIEnv * env , jclass c , jlong db , jlong start,jint startLen, jlong end, jint endLen ) {
    
    java_leveldb_vacuum( db,start,startLen,end,endLen ) ;
}

/** Iterator�쐬. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1iterator
  (JNIEnv * env , jclass c , jlong db ) {
    
    return java_leveldb_iterator( db ) ;
}

/** Iterator�N���[�Y. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1delete
  (JNIEnv * env , jclass c , jlong itr ) {
    
    java_leveldb_itr_delete( itr ) ;
}

/** Iterator�擪�Ɉړ�. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1first
  (JNIEnv * env , jclass c , jlong itr ) {
    
    java_leveldb_itr_first( itr ) ;
}

/** Iterator�Ō�Ɉړ�. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1last
  (JNIEnv * env , jclass c , jlong itr ) {
    
    java_leveldb_itr_last( itr ) ;
}

/** Iterator�V�[�N�ʒu�Ɉړ�. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1seek
  (JNIEnv * env , jclass c , jlong itr, jlong key, jint len ) {
    
    java_leveldb_itr_seek( itr,key,len ) ;
}

/** Iterator���݈ʒu�J�[�\���̏�񑶍݊m�F. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1valid
  (JNIEnv * env , jclass c , jlong itr ) {

    return java_leveldb_itr_valid( itr ) ;
}

/** Iterator�J�[�\�������Ɉړ�. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1next
  (JNIEnv * env , jclass c , jlong itr ) {
    
    java_leveldb_itr_next( itr ) ;
}

/** Iterator�J�[�\����O�Ɉړ�. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1before
  (JNIEnv * env , jclass c , jlong itr ) {
    
    java_leveldb_itr_before( itr ) ;
}

/** Iterator�J�[�\���ʒu��Key���擾. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1key
  (JNIEnv * env , jclass c , jlong itr , jlongArray out, jint bufLen ) {
    
    return java_leveldb_itr_key( env,itr,out,bufLen ) ;
}

/** Iterator�J�[�\���ʒu��Value���擾. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1itr_1value
  (JNIEnv * env , jclass c , jlong itr, jlongArray out, jint bufLen ) {
    
    return java_leveldb_itr_value( env,itr,out,bufLen ) ;
}



/** WriteBatch���𐶐�. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1create
  (JNIEnv * env , jclass c ) {
    
    return java_leveldb_wb_create() ;
}

/** WriteBatch�����T�C�Y�w��Ő���. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1create_1by_1size
  (JNIEnv * env , jclass c, jint size ) {
    return java_leveldb_wb_create_by_size( size ) ;
}

/** WriteBatch�����폜. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1destroy
  (JNIEnv * env , jclass c, jlong wb ) {
    
    java_leveldb_wb_destroy( wb ) ;
}

/** WriteBatch�����T�C�Y�w�肵�ăN���A. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1clear_1by_1size
  (JNIEnv * env , jclass c, jlong wb, jint size ) {
    
    java_leveldb_wb_clear_by_size( wb,size ) ;
}

/** WriteBatch�����N���A. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1clear
  (JNIEnv * env , jclass c, jlong wb ) {
    
    java_leveldb_wb_clear( wb ) ;
}

/** WriteBatch���ɒǉ��w��. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1put
  (JNIEnv * env , jclass c, jlong wb, jlong key, jint keyLen, jlong value, jint valueLen ) {
    
    java_leveldb_wb_put( wb,key,keyLen,value,valueLen ) ;
}

/** WriteBatch���ɍ폜�w��. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1remove
  (JNIEnv * env , jclass c, jlong wb, jlong key, jint len ) {
    
    java_leveldb_wb_remove( wb,key,len ) ;
}

/** WriteBatch���̓��e���擾. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1values
  (JNIEnv * env , jclass c, jlong wb ) {
    
    return java_leveldb_wb_values( wb ) ;
}

/** WriteBatch���̓��e�T�C�Y���擾. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1values_1size
  (JNIEnv * env , jclass c, jlong wb ) {
    
    return java_leveldb_wb_values_size( wb ) ;
}

/** WriteBatch����DB�ɔ��f. **/
JNIEXPORT jint JNICALL Java_org_maachang_leveldb_jni_leveldb_1wb_1flush
  (JNIEnv * env , jclass c, jlong db , jlong wb ) {
    
    return java_leveldb_wb_flush( db,wb ) ;
}

/** SnapShot�𐶐�. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1ss_1create
  (JNIEnv * env , jclass c, jlong db ) {
    
    return java_leveldb_createSnapShot( db ) ;
}

/** SnapShot��j��. **/
JNIEXPORT void JNICALL Java_org_maachang_leveldb_jni_leveldb_1ss_1destroy
  (JNIEnv * env , jclass c, jlong db, jlong ss ) {
    
    java_leveldb_releaseSnapShot( db,ss ) ;
}

/** SnapShot�pIterator�𐶐�. **/
JNIEXPORT jlong JNICALL Java_org_maachang_leveldb_jni_leveldb_1ss_1iterator
  (JNIEnv * env , jclass c, jlong db, jlong ss ) {
    
    return java_leveldb_getSnapShotIterator( db,ss ) ;
}

