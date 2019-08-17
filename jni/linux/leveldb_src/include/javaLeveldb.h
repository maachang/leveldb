#ifndef _JAVA_LEVEL_DB_INCLUDE_H_
#define _JAVA_LEVEL_DB_INCLUDE_H_

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Leveldb�j��. **/
void java_leveldb_destroy( jlong name,jint type,jint write_buffer_size,
    jint max_open_files,jint block_size,jint block_restart_interval ) ;

/** Leveldb����. **/
void java_leveldb_repair( jlong name,jint type,jint write_buffer_size,
    jint max_open_files,jint block_size,jint block_restart_interval ) ;

/** Leveldb�I�[�v��. **/
jlong java_leveldb_open( jlong name,jint type,jint write_buffer_size,
    jint max_open_files,jint block_size,jint block_restart_interval,jint block_cache ) ;

/** Leveldb�N���[�Y. **/
void java_leveldb_close( jlong db ) ;

/** Leveldb�v�f�Z�b�g. **/
jint java_leveldb_put( jlong db, jlong key, jint kLen, jlong value , jint vlen ) ;

/** Leveldb�v�f�擾. **/
jint java_leveldb_get( JNIEnv* env, jlong db , jlong key, jint len, jlongArray buf, jint bufLen ) ;

/** Leveldb�v�f�폜. **/
jint java_leveldb_remove( jlong db, jlong key, jint len ) ;

/** Leveldb��Ԏ擾. **/
jint java_leveldb_property( JNIEnv* env, jlong db , jlong cmd, jint len, jlongArray buf, jint bufLen ) ;

/** vacuum�I�ȏ���. **/
void java_leveldb_vacuum( jlong db, jlong start,jint startLen, jlong end, jint endLen ) ;

/** Iterator�쐬. **/
jlong java_leveldb_iterator( jlong db ) ;

/** Iterator�N���[�Y. **/
void java_leveldb_itr_delete( jlong itr ) ;

/** Iterator�擪�Ɉړ�. **/
void java_leveldb_itr_first( jlong itr ) ;

/** Iterator�Ō�Ɉړ�. **/
void java_leveldb_itr_last( jlong itr ) ;

/** Iterator�V�[�N�ʒu�Ɉړ�. **/
void java_leveldb_itr_seek( jlong itr, jlong key,jint len ) ;

/** Iterator���݈ʒu�J�[�\���̏�񑶍݊m�F. **/
jint java_leveldb_itr_valid( jlong itr ) ;

/** Iterator�J�[�\�������Ɉړ�. **/
void java_leveldb_itr_next( jlong itr ) ;

/** Iterator�J�[�\����O�Ɉړ�. **/
void java_leveldb_itr_before( jlong itr ) ;

/** Iterator�J�[�\���ʒu��Key���擾. **/
jint java_leveldb_itr_key( JNIEnv* env, jlong itr , jlongArray out, jint bufLen ) ;

/** Iterator�J�[�\���ʒu��Value���擾. **/
jint java_leveldb_itr_value( JNIEnv* env, jlong itr, jlongArray out, jint bufLen ) ;


/** WriteBatch�𐶐�. **/
jlong java_leveldb_wb_create() ;

/** WriteBatch���������T�C�Y��ݒ肵�Đ���. **/
jlong java_leveldb_wb_create_by_size( jint size ) ;

/** WriteBatch��j��. **/
void java_leveldb_wb_destroy( jlong wb ) ;

/** WriteBatch���������T�C�Y��ݒ肵�ăN���A. **/
void java_leveldb_wb_clear_by_size( jlong wb,jint size ) ;

/** WriteBatch���N���A. **/
void java_leveldb_wb_clear( jlong wb ) ;

/** WriteBatch�ɏ����Z�b�g. **/
void java_leveldb_wb_put( jlong wb, jlong key, jint kLen, jlong value , jint vLen ) ;

/** WriteBatch�ɏ����폜. **/
void java_leveldb_wb_remove( jlong wb, jlong key, jint len ) ;

/** WriteBatch�̓��e���擾. **/
jlong java_leveldb_wb_values( jlong wb ) ;

/** WriteBatch�̓��e�����擾. **/
jint java_leveldb_wb_values_size( jlong wb ) ;

/** WriteBatch��DB�ɔ��f. **/
jint java_leveldb_wb_flush( jlong db,jlong wb ) ;


/** SnapShot�𐶐�. **/
jlong java_leveldb_createSnapShot( jlong db ) ;

/** SnapShort�p��Iterator�𐶐�. **/
jlong java_leveldb_getSnapShotIterator( jlong db,jlong snapShot ) ;

/** �擾����SnapShort�����. **/
void java_leveldb_releaseSnapShot( jlong db,jlong snapShot ) ;


#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif /** _JAVA_LEVEL_DB_INCLUDE_H_ **/
