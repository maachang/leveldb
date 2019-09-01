#!/bin/sh

clear

LIB_VERSION="0.0.1"
LIB_NAME="leveldb-arm-${LIB_VERSION}.so"
rm -Rf ${LIB_NAME}

# c++.
#GCC_FLG="true"

#PLATFORM_CXXFLAGS=" -std=c++0x -fno-builtin-memcmp -pthread -DOS_LINUX -DLEVELDB_PLATFORM_POSIX -DLEVELDB_CSTDATOMIC_PRESENT -DLEVELDB_ATOMIC_PRESENT -DSNAPPY"
#PLATFORM_CCFLAGS=" -fno-builtin-memcmp -pthread -DOS_LINUX -DLEVELDB_PLATFORM_POSIX -DLEVELDB_CSTDATOMIC_PRESENT -DLEVELDB_ATOMIC_PRESENT -DSNAPPY"
PLATFORM_CXXFLAGS=" -std=c++0x -fno-builtin-memcmp -pthread -DOS_LINUX -DLEVELDB_PLATFORM_POSIX -DLEVELDB_CSTDATOMIC_PRESENT -DLEVELDB_ATOMIC_PRESENT -DLZ4"
PLATFORM_CCFLAGS=" -fno-builtin-memcmp -pthread -DOS_LINUX -DLEVELDB_PLATFORM_POSIX -DLEVELDB_CSTDATOMIC_PRESENT -DLEVELDB_ATOMIC_PRESENT -DLZ4"

PLATFORM_SHARED_CFLAGS="-fPIC"
PLATFORM_SHARED_LDFLAGS="-lstdc++ -shared -O2 -DNDEBUG"

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
INCLUDE_LIST="${INCLUDE_LIST} -I./snappy_src/"

${CC} ${PLATFORM_SHARED_LDFLAGS} ${PLATFORM_SHARED_CFLAGS} -I${JAVA_HOME}/include/ -I${JAVA_HOME}/include/linux/ ${INCLUDE_LIST} ${CPP_LIST} -o ${LIB_NAME}
