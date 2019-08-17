/* snappy java. */

#include "snappy_java.h"
#include "snappy.h"

void __cxa_pure_virtual() {
    
}

/* snappy init */
size_t _snappyMaxCompressedLength( size_t one_compress_length ) {
    return snappy::MaxCompressedLength(one_compress_length);
}

/* snappy compress */
int _snappyCompress( char* src,int src_len,char* dst,int* out ) {
    snappy::RawCompress(src, (size_t)src_len,dst, (size_t*)out) ;
    return 0 ;
}

/* snappy un compress */
size_t _snappyUncompress( char* src,int src_len,char* dst ) {
    size_t out;
    snappy::GetUncompressedLength((char*)src, (size_t)src_len, &out);
    bool ret = snappy::RawUncompress((char*)src, (size_t)src_len, (char*)dst);
    if( !ret ) {
        return ( size_t )-1 ;
    }
    return out ;
}

/** uncompress length */
void _snappyUncompressLength( char* src,int src_len,char* dst,int* out ) {
    snappy::GetUncompressedLength((char*)src, (size_t)src_len, (size_t*)out);
}

/* snappy un compress only */
size_t _snappyUncompressOnly( char* src,int src_len,char* dst ) {
    if( !snappy::RawUncompress((char*)src, (size_t)src_len, (char*)dst) ) {
        return ( size_t )-1 ;
    }
    return 0 ;
}
