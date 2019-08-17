/* lz4 java. */

#ifndef __LZ4_JAVA_H_INCLUDE_
#define __LZ4_JAVA_H_INCLUDE_

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* lz4 max compress length */
size_t _lz4MaxCompressedLength( size_t one_compress_length ) ;

/** lz4 uncompress length */
void _lz4UncompressLength( const char* dest,size_t* result ) ;

/* lz4 compress */
int _lz4Compress( const char* src,char* dest,int srcLen ) ;

/* lz4 un compress */
int _lz4Uncompress( const char* src,const size_t length,char* dest ) ;

/* lz4 un compress */
int _lz4UncompressOnly( const char* src,const size_t length,char* dest,int dest_len ) ;

#ifdef __cplusplus
}
#endif

#endif /* __LZ4_JAVA_H_INCLUDE_ */
