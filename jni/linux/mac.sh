#!/bin/sh

clear
LIB_VERSION = "0.0.1"
LIB_NAME="leveldb-${LIB_VERSION}.dylib"
rm -Rf ${LIB_NAME}

# cc
#GCC_FLG="true"

# For mac os version 10.12(macOS Sierra) and above, compile with 'LEVELDB_ATOMIC_PRESENT' parameter added.
# This is because 'OSMemoryBarrier' is deprecated in version 10.12(macOS Sierra) and above.

#PLATFORM_CCFLAGS=" -fno-builtin-memcmp -DOS_MACOSX -DLEVELDB_PLATFORM_POSIX -DSNAPPY -DLEVELDB_ATOMIC_PRESENT"
#PLATFORM_CXXFLAGS=" -fno-builtin-memcmp -DOS_MACOSX -DLEVELDB_PLATFORM_POSIX -DSNAPPY -DLEVELDB_ATOMIC_PRESENT"
PLATFORM_CCFLAGS=" -fno-builtin-memcmp -DOS_MACOSX -DLEVELDB_PLATFORM_POSIX -DLZ4 -DLEVELDB_ATOMIC_PRESENT"
PLATFORM_CXXFLAGS=" -fno-builtin-memcmp -DOS_MACOSX -DLEVELDB_PLATFORM_POSIX -DLZ4 -DLEVELDB_ATOMIC_PRESENT"

PLATFORM_SHARED_CFLAGS="-fPIC"
PLATFORM_SHARED_LDFLAGS="-dynamiclib -install_name ./ -O2 -DNDEBUG"

if [ ${GCC_FLG:--1} = "-1" ];
then
    CC="g++"
    PLATFORM_FLAGS=${PLATFORM_CXXFLAGS}
else
    CC="gcc"
    PLATFORM_FLAGS=${PLATFORM_CCFLAGS}
fi

CPP_LIST=""
CPP_LIST="${CPP_LIST} ./snappy_src/*.cc ./snappy_src/*.cpp"
CPP_LIST="${CPP_LIST} ./leveldb_src/db/*.cc"
#CPP_LIST="${CPP_LIST} ./leveldb_src/helpers/memenv/*.cc"
CPP_LIST="${CPP_LIST} ./leveldb_src/port/*.cc"
CPP_LIST="${CPP_LIST} ./leveldb_src/table/*.cc"
CPP_LIST="${CPP_LIST} ./leveldb_src/util/*.cc"
CPP_LIST="${CPP_LIST} ./leveldb_src/javaLeveldb.cpp"
CPP_LIST="${CPP_LIST} ./org_maachang_leveldb_jni.cc"

INCLUDE_LIST=""
INCLUDE_LIST="${INCLUDE_LIST} -I./leveldb_src/include/ ${PLATFORM_FLAGS}"
INCLUDE_LIST="${INCLUDE_LIST} -I./leveldb_src/ ${PLATFORM_FLAGS}"
INCLUDE_LIST="${INCLUDE_LIST} -I./snappy_src/ ${PLATFORM_FLAGS}"

${CC} ${PLATFORM_SHARED_LDFLAGS} ${PLATFORM_SHARED_CFLAGS} -I${JAVA_HOME}/include/ -I${JAVA_HOME}/include/darwin/ ${INCLUDE_LIST} ${CPP_LIST} -o ${LIB_NAME}
