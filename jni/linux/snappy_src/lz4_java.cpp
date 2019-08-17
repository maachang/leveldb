/* lz4 java. */

#include <memory.h>
#include "lz4_java.h"
#include "lz4.h"

/* lz4 max compress length */
size_t _lz4MaxCompressedLength(size_t one_compress_length) {
    return LZ4_COMPRESSBOUND(one_compress_length) ;
}

/** lz4 uncompress length */
void _lz4UncompressLength(const char* dest, size_t* result) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    memcpy(result, dest, 4) ;
#else
    size_t r  = (size_t)( ( dest[ 0 ] & 0x000000ff ) |
        ( ( dest[ 1 ] & 0x000000ff ) << 8 ) |
        ( ( dest[ 2 ] & 0x000000ff ) << 16 ) |
        ( ( dest[ 3 ] & 0x000000ff ) << 24 ) ) ;
    memcpy(result, &r, sizeof(size_t)) ;
#endif
}

/* lz4 compress */
int _lz4Compress(const char* src, char* dest, int srcLen) {
    //int ret = LZ4_compress(src, dest+4, srcLen) ;
    //int ret = LZ4_compress_default(src, dest+4, srcLen, LZ4_COMPRESSBOUND(srcLen)) ;
    int ret = LZ4_compress_fast(src, dest+4, srcLen, LZ4_COMPRESSBOUND(srcLen), 1);
    if(ret <= 0) {
        return ret ;
    }
    /** add header 4byte to src length. **/
#if __BYTE_ORDER == __LITTLE_ENDIAN
    memcpy(dest, &srcLen, 4) ;
#else
    dest[ 0 ] = srcLen & 0x000000ff ;
    dest[ 1 ] = ( srcLen & 0x0000ff00 ) >> 8 ;
    dest[ 2 ] = ( srcLen & 0x00ff0000 ) >> 16 ;
    dest[ 3 ] = ( srcLen & 0xff000000 ) >> 24 ;
#endif
    return ret + 4 ;
}

/* lz4 un compress */
int _lz4Uncompress(const char* src,const size_t length,char* dest) {
    size_t dest_len ;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    memcpy(&dest_len, src, 4) ;
#else
    dest_len = (size_t)( ( src[ 0 ] & 0x000000ff ) |
        ( ( src[ 1 ] & 0x000000ff ) << 8 ) |
        ( ( src[ 2 ] & 0x000000ff ) << 16 ) |
        ( ( src[ 3 ] & 0x000000ff ) << 24 ) ) ;
#endif
    //int res = LZ4_decompress_fast(src+4, dest, dest_len) ;
    int res = LZ4_decompress_safe(src+4, dest, length, dest_len);
    if(res < 0) {
        return -1 ;
    }
    return dest_len ;
}

/* lz4 un compress */
int _lz4UncompressOnly(const char* src,const size_t length,char* dest,int dest_len) {
    //int res = LZ4_decompress_fast(src+4, dest, dest_len) ;
    int res = LZ4_decompress_safe(src+4, dest, length, dest_len);
    if(res < 0) {
        return -1 ;
    }
    return dest_len ;
}
