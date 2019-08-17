// LevelDB Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// See port_example.h for documentation for the following types/functions.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 
//  * Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
//  * Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
//  * Neither the name of the University of California, Berkeley nor the
//    names of its contributors may be used to endorse or promote products
//    derived from this software without specific prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND ANY
// EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//

#ifndef STORAGE_LEVELDB_PORT_PORT_WIN_H_
#define STORAGE_LEVELDB_PORT_PORT_WIN_H_

#define snprintf _snprintf
#define close _close
#define fread_unlocked _fread_nolock

#include <string>
#include <stdint.h>

#include <cstring>
#include <list>


#if defined _MSC_VER
#define COMPILER_MSVC
#endif

#include "port/atomic_pointer.h"

#ifdef LEVELDB_WITH_SNAPPY
#include <snappy.h>
#endif
#ifdef LEVELDB_WITH_LZ4
#include <lz4_java.h>
#endif

typedef ptrdiff_t ssize_t;
typedef INT64 int64;

#ifdef min
#undef min
#endif

#ifdef small
#undef small
#endif

#define snprintf _snprintf
#define va_copy(a, b) do { (a) = (b); } while (0)

#if !defined(DISALLOW_COPY_AND_ASSIGN)
#  define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&); \
    void operator=(const TypeName&)
#endif 

#if defined _WIN32_WINNT_VISTA 
#define USE_VISTA_API
#endif

#pragma warning(disable:4996)
#pragma warning(disable:4018)
#pragma warning(disable:4355)
#pragma warning(disable:4244)
#pragma warning(disable:4800)
//#pragma warning(disable:4996)

namespace leveldb {
namespace port {

static const bool kLittleEndian = true;

class Event
{
public:
    Event(bool bSignal = true,bool ManualReset = false);
    ~Event();
    void Wait(DWORD Milliseconds = INFINITE);
    void Signal();
    void UnSignal();
private:
    HANDLE _hEvent;
    DISALLOW_COPY_AND_ASSIGN(Event);
};

class Mutex 
{
public:
    friend class CondVarNew;
    Mutex();
    ~Mutex();
    void Lock();
    void Unlock();
    BOOL TryLock();
    void AssertHeld();

private:
    CRITICAL_SECTION _cs;
    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

class AutoLock
{
public:
    explicit AutoLock(Mutex& mu) : _mu(mu)
    {
        _mu.Lock();
    }
    ~AutoLock()
    {
        _mu.Unlock();
    }
private:
    Mutex& _mu;
    DISALLOW_COPY_AND_ASSIGN(AutoLock);
};

#ifndef Scoped_Lock_Protect
#define Scoped_Lock_Protect(mu) AutoLock __auto_lock__(mu)
#endif

class AutoUnlock
{
public:
    explicit AutoUnlock(Mutex& mu) : _mu(mu)
    {
        _mu.Unlock();
    }
    ~AutoUnlock()
    {
        _mu.Lock();
    }
private:
    Mutex& _mu;
    DISALLOW_COPY_AND_ASSIGN(AutoUnlock);
};

#ifndef Scoped_Unlock_Protect
#define Scoped_Unlock_Protect(mu) AutoUnlock __auto_unlock__(mu)
#endif

//this class come from project Chromium
class CondVarOld
{
public:
    // Construct a cv for use with ONLY one user lock.
    explicit CondVarOld(Mutex* mu);
    ~CondVarOld();
    // Wait() releases the caller's critical section atomically as it starts to
    // sleep, and the reacquires it when it is signaled.
    void Wait();
    void timedWait(DWORD dwMilliseconds);
    // Signal() revives one waiting thread.
    void Signal();
    // SignalAll() revives all waiting threads.
    void SignalAll();

private:
    class Event {
    public:
        // Default constructor with no arguments creates a list container.
        Event();
        ~Event();

        // InitListElement transitions an instance from a container, to an element.
        void InitListElement();

        // Methods for use on lists.
        bool IsEmpty() const;
        void PushBack(Event* other);
        Event* PopFront();
        Event* PopBack();

        // Methods for use on list elements.
        // Accessor method.
        HANDLE handle() const;
        // Pull an element from a list (if it's in one).
        Event* Extract();

        // Method for use on a list element or on a list.
        bool IsSingleton() const;

    private:
        // Provide pre/post conditions to validate correct manipulations.
        bool ValidateAsDistinct(Event* other) const;
        bool ValidateAsItem() const;
        bool ValidateAsList() const;
        bool ValidateLinks() const;

        HANDLE handle_;
        Event* next_;
        Event* prev_;
        DISALLOW_COPY_AND_ASSIGN(Event);
    };
    // Note that RUNNING is an unlikely number to have in RAM by accident.
    // This helps with defensive destructor coding in the face of user error.
    enum RunState { SHUTDOWN = 0, RUNNING = 64213 };

    // Internal implementation methods supporting Wait().
    Event* GetEventForWaiting();
    void RecycleEvent(Event* used_event);

    RunState run_state_;

    // Private critical section for access to member data.
    Mutex internal_lock_;

    // Lock that is acquired before calling Wait().
    Mutex& user_lock_;

    // Events that threads are blocked on.
    Event waiting_list_;

    // Free list for old events.
    Event recycling_list_;
    int recycling_list_size_;

    // The number of allocated, but not yet deleted events.
    int allocation_counter_;
    DISALLOW_COPY_AND_ASSIGN(CondVarOld);
};

#if defined USE_VISTA_API

class CondVarNew
{
public:
    explicit CondVarNew(Mutex* mu);
    ~CondVarNew();
    void Wait();
    void Signal();
    void SignalAll();
private:
    CONDITION_VARIABLE _cv;
    Mutex* _mu;
};

typedef CondVarNew CondVar;

#else
typedef CondVarOld CondVar;
#endif


typedef void* OnceType;
#define LEVELDB_ONCE_INIT 0
extern void InitOnce(port::OnceType*, void (*initializer)());


inline bool Snappy_Compress(const char* input, size_t length,
                            ::std::string* output) {
#ifdef LEVELDB_WITH_SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#endif

#ifdef LEVELDB_WITH_LZ4
    output->resize( _lz4MaxCompressedLength( length ) ) ;
    size_t outLen = _lz4Compress( input,&(*output)[0],length ) ;
    output->resize( outLen ) ;
    return true ;
#endif

#ifndef LEVELDB_WITH_SNAPPY
#ifndef LEVELDB_WITH_LZ4
    return false;
#endif
#endif
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#ifdef LEVELDB_WITH_SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#endif

#ifdef LEVELDB_WITH_LZ4
    _lz4UncompressLength( input,result ) ;
    return true ;
#endif

#ifndef LEVELDB_WITH_SNAPPY
#ifndef LEVELDB_WITH_LZ4
    return false;
#endif
#endif

}

inline bool Snappy_Uncompress(const char* input, size_t length,
                              char* output) {
#ifdef LEVELDB_WITH_SNAPPY
  return snappy::RawUncompress(input, length, output);
#endif

#ifdef LEVELDB_WITH_LZ4
    _lz4Uncompress( input,length,output ) ;
    return true ;
#endif

#ifndef LEVELDB_WITH_SNAPPY
#ifndef LEVELDB_WITH_LZ4
    return false;
#endif
#endif
}

inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) {
  return false;
}

}
}

#endif  // STORAGE_LEVELDB_PORT_PORT_WIN_H_
