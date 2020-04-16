#include <stdlib.h>
#include <memory.h>

#include "javaLeveldb.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/slice.h"
#include "leveldb/iterator.h"
#include "leveldb/write_batch.h"
#include "leveldb/comparator.h"

/** 32bitからbyte変換. **/
//inline void encodeFixed32(char* buf, uint32_t value) {
//#if __BYTE_ORDER == __LITTLE_ENDIAN
//    memcpy(buf, &value, sizeof(value));
//#else
//    buf[0] = value & 0xff;
//    buf[1] = (value >> 8) & 0xff;
//    buf[2] = (value >> 16) & 0xff;
//    buf[3] = (value >> 24) & 0xff;
//#endif
//}

/** 64bitからbyte変換. **/
//inline void encodeFixed64(char* buf, uint64_t value) {
//#if __BYTE_ORDER == __LITTLE_ENDIAN
//    memcpy(buf, &value, sizeof(value));
//#else
//    buf[0] = value & 0xff;
//    buf[1] = (value >> 8) & 0xff;
//    buf[2] = (value >> 16) & 0xff;
//    buf[3] = (value >> 24) & 0xff;
//    buf[4] = (value >> 32) & 0xff;
//    buf[5] = (value >> 40) & 0xff;
//    buf[6] = (value >> 48) & 0xff;
//    buf[7] = (value >> 56) & 0xff;
//#endif
//}

/** byteから16bit変換. **/
inline uint32_t decodeFixed16(const char* ptr) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint32_t result;
    memcpy(&result, ptr, 2);
    return result;
#else
    return ((static_cast<uint32_t>(ptr[0]))
            | (static_cast<uint32_t>(ptr[1]) << 8)) ;
#endif
}

/** byteから32bit変換. **/
inline uint32_t decodeFixed32(const char* ptr) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
#else
    return ((static_cast<uint32_t>(ptr[0]))
            | (static_cast<uint32_t>(ptr[1]) << 8)
            | (static_cast<uint32_t>(ptr[2]) << 16)
            | (static_cast<uint32_t>(ptr[3]) << 24));
#endif
}

/** byteから64bit変換. **/
inline uint64_t decodeFixed64(const char* ptr) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
#else
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
#endif
}

/** 文字列検索用Comparator. **/
class StringKeyComparatr : public leveldb::Comparator {
public:
    StringKeyComparatr(){}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        const int aSize = a.size() ;
        const int bSize = b.size() ;
        const int min = ( aSize < bSize ) ? aSize : bSize ;
        const int ret = memcmp( a.data(), b.data(), min ) ;
        if( ret == 0 ) {
            return aSize - bSize ;
        }
        return ret ;
    }
    inline const char* Name() const { return (const char*)"strKey" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

/** 数値検索用Comparator. **/
/** 32bit整数比較処理. **/
class Number32KeyComparator : public leveldb::Comparator {
public:
    Number32KeyComparator(){}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        int src,dest ;
        src = decodeFixed32( ( const char* )a.data() ) ;
        dest = decodeFixed32( ( const char* )b.data() ) ;
        if( src < dest ) {
            return -1 ;
        }
        else if( src > dest ) {
            return 1 ;
        }
        return 0 ;
    }
    inline const char* Name() const { return (const char*)"n32Key" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

/** 64bit整数比較処理. **/
class Number64KeyComparator : public leveldb::Comparator {
public:
    Number64KeyComparator() {}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        jlong src,dest ;
        src = decodeFixed64( ( const char* )a.data() ) ;
        dest = decodeFixed64( ( const char* )b.data() ) ;
        if( src < dest ) {
            return -1 ;
        }
        else if( src > dest ) {
            return 1 ;
        }
        return 0 ;
    }
    inline const char* Name() const { return (const char*)"n64Key" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

/** 文字列＋文字列用Comparator. **/
class StringStringKeyComparatr : public leveldb::Comparator {
public:
    StringStringKeyComparatr(){}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        int aLen, bLen, aSize, bSize, nA, nB, min, ret ;

        const char* aData = a.data() ;
        const char* bData = b.data() ;

        aLen = a.size() - 4 ;
        bLen = b.size() - 4 ;
        
        // 先頭の文字列長は、size() - 4 の位置に 32bit整数で格納されている.
        aSize = decodeFixed32( aData + aLen ) ;
        bSize = decodeFixed32( bData + bLen ) ;
        min = ( aSize < bSize ) ? aSize : bSize ;
        if( ( ret = memcmp( aData, bData, min ) ) == 0 ) {
            if(aSize != bSize) {
                return aSize - bSize ;
            }
        } else {
            return ret;
        }
        
        // 次の領域に対して文字列でチェックする.
        // 純粋な文字列をチェックして、その値が同一の場合は
        // 元の長さで判別する.
        nA = aLen - aSize ;
        nB = bLen - bSize ;
        min = ( nA < nB ) ? nA : nB ;
        if( ( ret = memcmp( aData + aSize, bData + bSize, min ) ) == 0 ) {
            return nA - nB ;
        }
        return ret ;
    }
    inline const char* Name() const { return (const char*)"strStrKey" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

/** 文字列＋Number32用Comparator. **/
class StringNumber32KeyComparatr : public leveldb::Comparator {
public:
    StringNumber32KeyComparatr(){}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        int aLen, bLen, aSize, bSize, nA, nB, min, ret ;
        
        const char* aData = a.data() ;
        const char* bData = b.data() ;
        
        aLen = a.size() - 4 ;
        bLen = b.size() - 4 ;
        
        // 先頭の文字列長は、size() - 4 の位置に 32bit整数で格納されている.
        aSize = decodeFixed32( aData + aLen ) ;
        bSize = decodeFixed32( bData + bLen ) ;
        min = ( aSize < bSize ) ? aSize : bSize ;
        if( ( ret = memcmp( aData, bData, min ) ) == 0 ) {
            if(aSize != bSize) {
                return aSize - bSize ;
            }
        } else {
            return ret;
        }
        
        // 次の領域に対して32ビット整数でチェックする.
        nA = decodeFixed32( aData + aSize ) ;
        nB = decodeFixed32( bData + bSize ) ;
        if( nA != nB ) {
            return ( nA < nB ) ? -1 : 1 ;
        }
        return 0 ;
    }
    inline const char* Name() const { return (const char*)"strN32Key" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

/** 文字列＋Number64用Comparator. **/
class StringNumber64KeyComparatr : public leveldb::Comparator {
public:
    StringNumber64KeyComparatr(){}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        int aLen, bLen, aSize, bSize, min, ret ;
        jlong nnA, nnB ;

        const char* aData = a.data() ;
        const char* bData = b.data() ;
        
        aLen = a.size() - 4 ;
        bLen = b.size() - 4 ;

        // 先頭の文字列長は、size() - 4 の位置に 32bit整数で格納されている.
        aSize = decodeFixed32( aData + aLen ) ;
        bSize = decodeFixed32( bData + bLen ) ;
        min = ( aSize < bSize ) ? aSize : bSize ;
        if( ( ret = memcmp( aData, bData, min ) ) == 0 ) {
            if(aSize != bSize) {
                return aSize - bSize ;
            }
        } else {
            return ret;
        }
        
        // 次の領域に対して64ビット整数でチェックする.
        nnA = decodeFixed64( aData + aSize ) ;
        nnB = decodeFixed64( bData + bSize ) ;
        if( nnA != nnB ) {
            return ( nnA < nnB ) ? -1 : 1 ;
        }
        return 0 ;
    }
    inline const char* Name() const { return (const char*)"strN64Key" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

/** Number32＋文字列用Comparator. **/
class Number32StringKeyComparatr : public leveldb::Comparator {
public:
    Number32StringKeyComparatr(){}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        int aSize,bSize,nA,nB,min,ret ;

        const char* aData = a.data() ;
        const char* bData = b.data() ;
        
        // 次の領域に対して32ビット整数でチェックする.
        nA = decodeFixed32( aData ) ;
        nB = decodeFixed32( bData ) ;
        if( nA != nB ) {
            return ( nA < nB ) ? -1 : 1 ;
        }
        
        // 次に文字列で検索.
        // 先頭の４バイトを無視した開始位置と長さで比較処理.
        aSize = a.size() - 4 ;
        bSize = b.size() - 4 ;
        min = ( aSize < bSize ) ? aSize : bSize ;
        if( ( ret = memcmp( aData+4, bData+4, min ) ) == 0 ) {
            return aSize - bSize ;
        }
        return ret ;
    }
    inline const char* Name() const { return (const char*)"n32StrKey" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

/** Number32＋Number32用Comparator. **/
class Number32Number32KeyComparatr : public leveldb::Comparator {
public:
    Number32Number32KeyComparatr(){}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        int aSize,bSize,nA,nB ;
        const char* aData = a.data() ;
        const char* bData = b.data() ;
        
        // 次の領域に対して32ビット整数でチェックする.
        nA = decodeFixed32( aData ) ;
        nB = decodeFixed32( bData ) ;
        if( nA != nB ) {
            return ( nA < nB ) ? -1 : 1 ;
        }
        
        // 次の領域に対して32ビット整数でチェックする.
        // 先頭の４バイトを無視した開始位置と長さで比較処理.
        nA = decodeFixed32( aData + 4 ) ;
        nB = decodeFixed32( bData + 4 ) ;
        if( nA != nB ) {
            return ( nA < nB ) ? -1 : 1 ;
        }
        return 0 ;
    }
    inline const char* Name() const { return (const char*)"n32N32Key" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

/** Number32＋Number64用Comparator. **/
class Number32Number64KeyComparatr : public leveldb::Comparator {
public:
    Number32Number64KeyComparatr(){}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        int aSize,bSize,nA,nB ;
        jlong nnA,nnB ;
        
        const char* aData = a.data() ;
        const char* bData = b.data() ;
        
        // 次の領域に対して32ビット整数でチェックする.
        nA = decodeFixed32( aData ) ;
        nB = decodeFixed32( bData ) ;
        if( nA != nB ) {
            return ( nA < nB ) ? -1 : 1 ;
        }
        
        // 次の領域に対して64ビット整数でチェックする.
        // 先頭の４バイトを無視した開始位置と長さで比較処理.
        nnA = decodeFixed64( aData + 4 ) ;
        nnB = decodeFixed64( bData + 4 ) ;
        if( nnA != nnB ) {
            return ( nnA < nnB ) ? -1 : 1 ;
        }
        return 0 ;
    }
    inline const char* Name() const { return (const char*)"n32N64Key" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

/** Number64＋文字列用Comparator. **/
class Number64StringKeyComparatr : public leveldb::Comparator {
public:
    Number64StringKeyComparatr(){}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        int aSize,bSize,min,ret ;
        jlong nnA,nnB ;

        const char* aData = a.data() ;
        const char* bData = b.data() ;
        
        // 次の領域に対して64ビット整数でチェックする.
        nnA = decodeFixed64( aData ) ;
        nnB = decodeFixed64( bData ) ;
        if( nnA != nnB ) {
            return ( nnA < nnB ) ? -1 : 1 ;
        }
        
        // 次に文字列で検索.
        // 先頭の8バイトを無視した開始位置と長さで比較処理.
        aSize = a.size() - 8 ;
        bSize = b.size() - 8 ;
        min = ( aSize < bSize ) ? aSize : bSize ;
        if( ( ret = memcmp( aData+8, bData+8, min ) ) == 0 ) {
            return aSize - bSize ;
        }
        return ret ;
    }
    inline const char* Name() const { return (const char*)"n64StrKey" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

/** Number64＋Number32用Comparator. **/
class Number64Number32KeyComparatr : public leveldb::Comparator {
public:
    Number64Number32KeyComparatr(){}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        int nA,nB ;
        jlong nnA,nnB ;

        const char* aData = a.data() ;
        const char* bData = b.data() ;
        
        // 次の領域に対して64ビット整数でチェックする.
        nnA = decodeFixed64( aData ) ;
        nnB = decodeFixed64( bData ) ;
        if( nnA != nnB ) {
            return ( nnA < nnB ) ? -1 : 1 ;
        }
        
        // 次の領域に対して32ビット整数でチェックする.
        // 先頭の8バイトを無視した開始位置と長さで比較処理.
        nA = decodeFixed32( aData + 8 ) ;
        nB = decodeFixed32( bData + 8 ) ;
        if( nA != nB ) {
            return ( nA < nB ) ? -1 : 1 ;
        }
        return 0 ;
    }
    inline const char* Name() const { return (const char*)"n64N32Key" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

/** Number64＋Number64用Comparator. **/
class Number64Number64KeyComparatr : public leveldb::Comparator {
public:
    Number64Number64KeyComparatr(){}
    // if a < b: negative result -1
    // if a > b: positive result +1 
    // else: zero result          0
    inline int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        jlong nnA,nnB ;

        const char* aData = a.data() ;
        const char* bData = b.data() ;
        
        // 次の領域に対して64ビット整数でチェックする.
        nnA = decodeFixed64( aData ) ;
        nnB = decodeFixed64( bData ) ;
        if( nnA != nnB ) {
            return ( nnA < nnB ) ? -1 : 1 ;
        }
        
        // 次の領域に対して32ビット整数でチェックする.
        // 先頭の8バイトを無視した開始位置と長さで比較処理.
        nnA = decodeFixed64( aData + 8 ) ;
        nnB = decodeFixed64( bData + 8 ) ;
        if( nnA != nnB ) {
            return ( nnA < nnB ) ? -1 : 1 ;
        }
        return 0 ;
    }
    inline const char* Name() const { return (const char*)"n64N64Key" ; } ;
    inline void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    inline void FindShortSuccessor(std::string*) const {}
} ;

// １キー用.
static StringKeyComparatr sortString = StringKeyComparatr() ;
static Number32KeyComparator sortN32 = Number32KeyComparator() ;
static Number64KeyComparator sortN64 = Number64KeyComparator() ;

// 文字列-２キー用.
static StringStringKeyComparatr sortStrStr = StringStringKeyComparatr() ;
static StringNumber32KeyComparatr sortStrN32 = StringNumber32KeyComparatr() ;
static StringNumber64KeyComparatr sortStrN64 = StringNumber64KeyComparatr() ;

// Number32-２キー用.
static Number32StringKeyComparatr sortN32Str = Number32StringKeyComparatr() ;
static Number32Number32KeyComparatr sortN32N32 = Number32Number32KeyComparatr() ;
static Number32Number64KeyComparatr sortN32N64 = Number32Number64KeyComparatr() ;

// Number64-２キー用.
static Number64StringKeyComparatr sortN64Str = Number64StringKeyComparatr() ;
static Number64Number32KeyComparatr sortN64N32 = Number64Number32KeyComparatr() ;
static Number64Number64KeyComparatr sortN64N64 = Number64Number64KeyComparatr() ;

/** オプション定義. **/
inline void setOption( leveldb::Options* op,jint type,jint write_buffer_size,
    jint max_open_files,jint block_size,jint block_restart_interval,jint block_cache ) {
    
    if( write_buffer_size != -1 ) {
        op->write_buffer_size = (size_t)write_buffer_size ;
    }
    if( max_open_files != -1 ) {
        op->max_open_files = (int)max_open_files ;
    }
    if( block_size != -1 ) {
        op->block_size = (size_t)block_size ;
    }
    if( block_restart_interval != -1 ) {
        op->block_restart_interval = (int)block_restart_interval ;
    }
    op->create_if_missing = true ;
    
    // cahce はMByte単位.
    if( block_cache != -1 ) {
        op->block_cache = leveldb::NewLRUCache(block_cache * 1048576) ;
    }
    
    /** 
     * type : 0 =>  string.
     * type : 1 =>  number32.
     * type : 2 =>  number64.
     *
     * type : 3 =>  string-string.
     * type : 4 =>  string-number32.
     * type : 5 =>  string-number64.
     *
     * type : 6 =>  number32-string.
     * type : 7 =>  number32-number32.
     * type : 8 =>  number32-number64.
     *
     * type : 9 =>  number64-string.
     * type : 10 => number64-number32.
     * type : 11 => number64-number64.
     */
    switch( type ) {
        case 0 : op->comparator = &sortString ; break ;
        case 1 : op->comparator = &sortN32 ; break ;
        case 2 : op->comparator = &sortN64 ; break ;
        
        case 3 : op->comparator = &sortStrStr ; break ;
        case 4 : op->comparator = &sortStrN32 ; break ;
        case 5 : op->comparator = &sortStrN64 ; break ;
        
        case 6 : op->comparator = &sortN32Str ; break ;
        case 7 : op->comparator = &sortN32N32 ; break ;
        case 8 : op->comparator = &sortN32N64 ; break ;
        
        case 9 : op->comparator = &sortN64Str ; break ;
        case 10 : op->comparator = &sortN64N32 ; break ;
        case 11 : op->comparator = &sortN64N64 ; break ;
    }
}

/** Leveldb破棄. **/
void java_leveldb_destroy( jlong name,jint type,jint write_buffer_size,
    jint max_open_files,jint block_size,jint block_restart_interval ) {
    
    std::string dbName((char*)name) ;
    
    leveldb::Options op ;
    setOption( &op,type,write_buffer_size,max_open_files,
        block_size,block_restart_interval,-1 ) ;
    
    leveldb::DestroyDB( dbName,op ) ;
}

/** Leveldb壊れたデータを修復. **/
void java_leveldb_repair( jlong name,jint type,jint write_buffer_size,
    jint max_open_files,jint block_size,jint block_restart_interval ) {
    
    std::string dbName((char*)name) ;
    
    leveldb::Options op ;
    setOption( &op,type,write_buffer_size,max_open_files,
        block_size,block_restart_interval,-1 ) ;
    
    leveldb::RepairDB( dbName,op ) ;
}

/** Leveldbオープン. **/
jlong java_leveldb_open( jlong name,jint type,jint write_buffer_size,
    jint max_open_files,jint block_size,jint block_restart_interval,jint block_cache ) {
    
    std::string dbName((char*)name) ;
    
    leveldb::Options op ;
    setOption( &op,type,write_buffer_size,max_open_files,
        block_size,block_restart_interval,block_cache ) ;
    
    leveldb::DB* db ;
    
    leveldb::Status status = leveldb::DB::Open(op, dbName, &db);
    if( status.ok() ) {
        return (jlong)db ;
    }
    return 0 ;
}

/** Leveldbクローズ. **/
void java_leveldb_close( jlong db ) {
    leveldb::DB* n = (leveldb::DB*)db ;
    if( n ) {
        delete n ;
    }
}

/** Leveldb要素セット. **/
jint java_leveldb_put( jlong db, jlong key, jint kLen, jlong value , jint vLen ) {
    leveldb::DB* vdb = (leveldb::DB*)db ;
    if( vdb ) {
        leveldb::Status status = vdb->Put(leveldb::WriteOptions(),
            leveldb::Slice( (const char*)key,kLen ),
            leveldb::Slice( (const char*)value,vLen ) ) ;
        if( !status.ok() ) {
            return -1 ;
        }
        return 0 ;
    }
    return -1 ;
}

/** Leveldb要素取得. **/
jint java_leveldb_get( JNIEnv* env, jlong db , jlong key, jint len, jlongArray buf, jint bufLen ) {
    leveldb::DB* vdb = (leveldb::DB*)db ;
    if( vdb ) {
        std::string v ;
        
        leveldb::Status status = vdb->Get( leveldb::ReadOptions(),
            leveldb::Slice( (const char*)key,len ),&v ) ;
        if( !status.ok() ) {
            return -1 ;
        }
        else if( v.size() == 0 ) {
            return 0 ;
        }
        
        jlong n ;
        env->GetLongArrayRegion( buf,0,1,&n ) ;
        char* b = (char*)n ;
        
        if( v.size() > bufLen ) {
            b = (char*)malloc( v.size() ) ;
            n = (jlong)b ;
            env->SetLongArrayRegion( buf,0,1,&n ) ;
        }
        memcpy( b,v.c_str(),v.size() ) ;
        return v.size() ;
    }
    return -1 ;
}

/** Leveldb要素削除. **/
jint java_leveldb_remove( jlong db, jlong key, jint len ) {
    leveldb::DB* vdb = (leveldb::DB*)db ;
    if( vdb ) {
        leveldb::Status status = vdb->Delete( leveldb::WriteOptions(),
            leveldb::Slice( (const char*)key,len ) ) ;
        if( !status.ok() ) {
            return -1 ;
        }
        return 0 ;
    }
    return -1 ;
}

/** Leveldb状態取得. **/
jint java_leveldb_property( JNIEnv* env, jlong db , jlong cmd, jint len, jlongArray buf, jint bufLen ) {
    leveldb::DB* vdb = (leveldb::DB*)db ;
    if( vdb ) {
        std::string v ;
        
        if( !vdb->GetProperty( leveldb::Slice( (const char*)cmd,len ),&v ) ) {
            return -1 ;
        }
        else if( v.size() == 0 ) {
            return 0 ;
        }
        
        jlong n ;
        env->GetLongArrayRegion( buf,0,1,&n ) ;
        char* b = (char*)n ;
        
        if( v.size() > bufLen ) {
            b = (char*)malloc( v.size() ) ;
            n = (jlong)b ;
            env->SetLongArrayRegion( buf,0,1,&n ) ;
        }
        memcpy( b,v.c_str(),v.size() ) ;
        return v.size() ;
    }
    return -1 ;
}

/** vacuum処理. **/
void java_leveldb_vacuum( jlong db, jlong start,jint startLen, jlong end, jint endLen ) {
    leveldb::DB* vdb = (leveldb::DB*)db ;
    leveldb::Slice* startKey = NULL ;
    leveldb::Slice* endKey = NULL ;
    if( vdb ) {
        if( start != 0 && startLen != 0 ) {
            startKey = new leveldb::Slice( (const char*)start,startLen ) ;
        }
        if( end != 0 && endLen != 0 ) {
            endKey = new leveldb::Slice( (const char*)end,endLen ) ;
        }
        vdb->CompactRange( startKey,endKey ) ;
        if( startKey != NULL ) {
            delete startKey ;
        }
        if( endKey != NULL ) {
            delete endKey ;
        }
    }
}

/** Iterator作成. **/
jlong java_leveldb_iterator( jlong db ) {
    leveldb::DB* vdb = (leveldb::DB*)db ;
    if( vdb ) {
        return (jlong)vdb->NewIterator( leveldb::ReadOptions() ) ;
    }
    return 0 ;
}

/** Iteratorクローズ. **/
void java_leveldb_itr_delete( jlong itr ) {
    leveldb::Iterator* n = (leveldb::Iterator*)itr ;
    if( n ) {
        delete n ;
    }
}

/** Iterator先頭に移動. **/
void java_leveldb_itr_first( jlong itr ) {
    leveldb::Iterator* it = (leveldb::Iterator*)itr ;
    if( it ) {
        it->SeekToFirst() ;
    }
}

/** Iterator最後に移動. **/
void java_leveldb_itr_last( jlong itr ) {
    leveldb::Iterator* it = (leveldb::Iterator*)itr ;
    if( it ) {
        it->SeekToLast() ;
    }
}

/** Iteratorシーク位置に移動. **/
void java_leveldb_itr_seek( jlong itr, jlong key,jint len ) {
    leveldb::Iterator* it = (leveldb::Iterator*)itr ;
    if( it ) {
        it->Seek( leveldb::Slice( (const char*)key,len ) ) ;
    }
}

/** Iterator現在位置カーソルの情報存在確認. **/
jint java_leveldb_itr_valid( jlong itr ) {
    leveldb::Iterator* it = (leveldb::Iterator*)itr ;
    if( it ) {
        if( it->Valid() ) {
            return 1 ;
        }
        return 0 ;
    }
    return -1 ;
}

/** Iteratorカーソルを次に移動. **/
void java_leveldb_itr_next( jlong itr ) {
    leveldb::Iterator* it = (leveldb::Iterator*)itr ;
    if( it && it->Valid() ) {
        it->Next() ;
    }
}

/** Iteratorカーソルを前に移動. **/
void java_leveldb_itr_before( jlong itr ) {
    leveldb::Iterator* it = (leveldb::Iterator*)itr ;
    if( it && it->Valid() ) {
        it->Prev() ;
    }
}

/** Iteratorカーソル位置のKeyを取得. **/
jint java_leveldb_itr_key( JNIEnv* env, jlong itr , jlongArray out, jint bufLen ) {
    leveldb::Iterator* it = (leveldb::Iterator*)itr ;
    
    if( !it || !it->Valid() ) {
        return -1 ;
    }

    leveldb::Slice ret = it->key() ;
    if( !it->status().ok() ) {
        return -1 ;
    }
    else if( ret.size() <= 0 ) {
        return 0 ;
    }
    
    jlong n ;
    env->GetLongArrayRegion( out,0,1,&n ) ;
    char* b = (char*)n ;
    
    if( ret.size() > bufLen ) {
        b = (char*)malloc( ret.size() ) ;
        n = (jlong)b ;
        env->SetLongArrayRegion( out,0,1,&n ) ;
    }
    memcpy( b,ret.data(),ret.size() ) ;
    return ret.size() ;
}

/** Iteratorカーソル位置のValueを取得. **/
jint java_leveldb_itr_value( JNIEnv* env, jlong itr, jlongArray out, jint bufLen ) {
    leveldb::Iterator* it = (leveldb::Iterator*)itr ;
    
    if( !it || !it->Valid() ) {
        return -1 ;
    }
    
    leveldb::Slice ret = it->value() ;
    if( !it->status().ok() ) {
        return -1 ;
    }
    else if( ret.size() <= 0 ) {
        return 0 ;
    }
    
    jlong n ;
    env->GetLongArrayRegion( out,0,1,&n ) ;
    char* b = (char*)n ;
    
    if( ret.size() > bufLen ) {
        b = (char*)malloc( ret.size() ) ;
        n = (jlong)b ;
        env->SetLongArrayRegion( out,0,1,&n ) ;
    }
    memcpy( b,ret.data(),ret.size() ) ;
    return ret.size() ;
}

/** WriteBatchを生成. **/
jlong java_leveldb_wb_create() {
    return (jlong)( new leveldb::WriteBatch() ) ;
}

/** WriteBatchをメモリサイズを設定して生成. **/
jlong java_leveldb_wb_create_by_size( jint size ) {
    return (jlong)( new leveldb::WriteBatch( (const size_t)size ) ) ;
}

/** WriteBatchを破棄. **/
void java_leveldb_wb_destroy( jlong wb ) {
    leveldb::WriteBatch* n = (leveldb::WriteBatch*)wb ;
    if( n ) {
        delete n ;
    }
}

/** WriteBatchをメモリサイズを設定してクリア. **/
void java_leveldb_wb_clear_by_size( jlong wb,jint size ) {
    leveldb::WriteBatch* n = (leveldb::WriteBatch*)wb ;
    if( n ) {
        n->Clear( (const size_t)size ) ;
    }
}

/** WriteBatchをクリア. **/
void java_leveldb_wb_clear( jlong wb ) {
    leveldb::WriteBatch* n = (leveldb::WriteBatch*)wb ;
    if( n ) {
        n->Clear() ;
    }
}

/** WriteBatchに情報をセット. **/
void java_leveldb_wb_put( jlong wb, jlong key, jint kLen, jlong value , jint vLen ) {
    leveldb::WriteBatch* n = (leveldb::WriteBatch*)wb ;
    if( n ) {
        n->Put( leveldb::Slice( (const char*)key,kLen ),
        leveldb::Slice( (const char*)value,vLen ) ) ;
    }
}

/** WriteBatchに情報を削除. **/
void java_leveldb_wb_remove( jlong wb, jlong key, jint len ) {
    leveldb::WriteBatch* n = (leveldb::WriteBatch*)wb ;
    if( n ) {
        n->Delete( leveldb::Slice( (const char*)key,len )  ) ;
    }
}

/** WriteBatchの内容を取得. **/
jlong java_leveldb_wb_values( jlong wb ) {
    leveldb::WriteBatch* n = (leveldb::WriteBatch*)wb ;
    if( n ) {
        return (jlong)(n->Values()) ;
    }
    return (jlong)0 ;
}

/** WriteBatchの内容長を取得. **/
jint java_leveldb_wb_values_size( jlong wb ) {
    leveldb::WriteBatch* n = (leveldb::WriteBatch*)wb ;
    if( n ) {
        return (jint)(n->ValuesSize()) ;
    }
    return (jint)0 ;
}

/** WriteBatchをDBに反映. **/
jint java_leveldb_wb_flush( jlong db,jlong wb ) {
    leveldb::DB* vdb = (leveldb::DB*)db ;
    leveldb::WriteBatch* n = (leveldb::WriteBatch*)wb ;
    if( vdb && n ) {
        leveldb::Status status = vdb->Write(leveldb::WriteOptions(),n ) ;
        if( !status.ok() ) {
            return -1 ;
        }
        return 0 ;
    }
    return -1 ;
}

/** SnapShotを生成. **/
jlong java_leveldb_createSnapShot( jlong db ) {
    leveldb::DB* vdb = (leveldb::DB*)db ;
    if( vdb ) {
        leveldb::ReadOptions* ret = new leveldb::ReadOptions() ;
        ret->snapshot = vdb->GetSnapshot() ;
        return (jlong)ret ;
    }
    return (jlong)0 ;
}

/** SnapShort用のIteratorを生成. **/
jlong java_leveldb_getSnapShotIterator( jlong db,jlong snapShot ) {
    leveldb::DB* vdb = (leveldb::DB*)db ;
    if( vdb ) {
        leveldb::ReadOptions* op = (leveldb::ReadOptions*)snapShot ;
        if( op ) {
            return (jlong)vdb->NewIterator( *op ) ;
        }
    }
    return 0 ;
}

/** 取得したSnapShortを解放. **/
void java_leveldb_releaseSnapShot( jlong db,jlong snapShot ) {
    leveldb::DB* vdb = (leveldb::DB*)db ;
    if( vdb ) {
        leveldb::ReadOptions* op = (leveldb::ReadOptions*)snapShot ;
        if( op ) {
            vdb->ReleaseSnapshot( op->snapshot ) ;
            op->snapshot = NULL ;
        }
        delete op ;
    }
}
