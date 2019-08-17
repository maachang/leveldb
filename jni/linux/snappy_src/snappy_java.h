/* snappy java. */

#ifndef __SNAPPY_JAVA_H_INCLUDE_
#define __SNAPPY_JAVA_H_INCLUDE_

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

void __cxa_pure_virtual() ;

/* snappy max compressed length */
size_t _snappyMaxCompressedLength( size_t one_compress_length ) ;

/* snappy compress */
int _snappyCompress( char* src,int src_len,char* dst,int* out ) ;

/* snappy un compress */
size_t _snappyUncompress( char* src,int src_len,char* dst ) ;

/** uncompress length */
void _snappyUncompressLength( char* src,int src_len,char* dst,int* out ) ;

/* snappy un compress only */
size_t _snappyUncompressOnly( char* src,int src_len,char* dst ) ;


#ifdef __cplusplus
}
#endif

#endif /* __SNAPPY_JAVA_H_INCLUDE_ */
