/**
 * C++ record manager implementation (PODC 2015) by Trevor Brown.
 * 
 * Copyright (C) 2015 Trevor Brown
 *
 */

#ifndef RECOVERY_MANAGER_H
#define	RECOVERY_MANAGER_H

#include <setjmp.h>

#ifndef VERBOSE
    #define VERBOSE if(0)
#endif
    
#include <cassert>
#include <csignal>
#include "globals.h"

// for crash recovery
/*PAD;*/
static pthread_key_t pthreadkey;
static struct sigaction ___act;
static void *___singleton = NULL;
/*PAD;*/
extern pthread_key_t pthreadkey;
extern struct sigaction ___act;
extern void *___singleton;

static pthread_t registeredThreads[MAX_THREADS_POW2];
static void *errnoThreads[MAX_THREADS_POW2];
static sigjmp_buf *setjmpbuffers;
/*PAD;*/
extern pthread_t registeredThreads[MAX_THREADS_POW2];
extern void *errnoThreads[MAX_THREADS_POW2];
extern sigjmp_buf *setjmpbuffers;

#define MAX_THREAD_ADDR 10000

#ifdef CRASH_RECOVERY_USING_SETJMP
#define CHECKPOINT_AND_RUN_UPDATE(tid, finishedbool) \
    if (MasterRecordMgr::supportsCrashRecovery() && sigsetjmp(setjmpbuffers[(tid)], 0)) { \
        recordmgr->endOp((tid)); \
        (finishedbool) = recoverAnyAttemptedSCX((tid), -1); \
        recordmgr->recoveryMgr->unblockCrashRecoverySignal(); \
    } else
#define CHECKPOINT_AND_RUN_QUERY(tid) \
    if (MasterRecordMgr::supportsCrashRecovery() && sigsetjmp(setjmpbuffers[(tid)], 0)) { \
        recordmgr->endOp((tid)); \
        recordmgr->recoveryMgr->unblockCrashRecoverySignal(); \
    } else
#endif

// warning: this crash recovery code will only work if you've created a SINGLE instance of bst during an execution.
// there are ways to make it work for multiple instances; i just haven't done that.
template <class MasterRecordMgr>
void crashhandler(int signum, siginfo_t *info, void *uctx) {
    MasterRecordMgr * const recordmgr = (MasterRecordMgr * const) ___singleton;
#ifdef SIGHANDLER_IDENTIFY_USING_PTHREAD_GETSPECIFIC
    int tid = (int) ((long) pthread_getspecific(pthreadkey));
#endif
    TRACE COUTATOMICTID("received signal "<<signum<<std::endl);

    // if i'm active (not in a quiescent state), i must throw an exception
    // and clean up after myself, instead of continuing my operation.
    __sync_synchronize();
    if (!recordmgr->isQuiescent(tid)) {
#ifdef PERFORM_RESTART_IN_SIGHANDLER
        recordmgr->endOp(tid);
        __sync_synchronize();
    #ifdef CRASH_RECOVERY_USING_SETJMP
        siglongjmp(setjmpbuffers[tid], 1);
    #endif
#endif
    }
    // otherwise, i simply continue my operation as if nothing happened.
    // this lets me behave nicely when it would be dangerous for me to be
    // restarted (being in a Q state is analogous to having interrupts 
    // disabled in an operating system kernel; however, whereas disabling
    // interrupts blocks other processes' progress, being in a Q state
    // implies that you cannot block the progress of any other thread.)
}

template <class MasterRecordMgr>
class RecoveryMgr {
public:
    PAD;
    const int NUM_PROCESSES;
    const int neutralizeSignal;
    PAD;
    
    inline int getTidInefficient(const pthread_t me) {
        int tid = -1;
        for (int i=0;i<NUM_PROCESSES;++i) {
            if (pthread_equal(registeredThreads[i], me)) {
                tid = i;
            }
        }
        // fail to find my tid -- should be impossible
        if (tid == -1) {
            COUTATOMIC("THIS SHOULD NEVER HAPPEN"<<std::endl);
            assert(false);
            exit(-1);
        }
        return tid;
    }
    inline int getTidInefficientErrno() {
        int tid = -1;
        for (int i=0;i<NUM_PROCESSES;++i) {
            // here, we use the fact that errno is defined to be a thread local variable
            if (&errno == errnoThreads[i]) {
                tid = i;
            }
        }
        // fail to find my tid -- should be impossible
        if (tid == -1) {
            COUTATOMIC("THIS SHOULD NEVER HAPPEN"<<std::endl);
            assert(false);
            exit(-1);
        }
        return tid;
    }
    inline int getTid_pthread_getspecific() {
        void * result = pthread_getspecific(pthreadkey);
        if (!result) {
            assert(false);
            COUTATOMIC("ERROR: failed to get thread id using pthread_getspecific"<<std::endl);
            exit(-1);
        }
        return (int) ((long) result);
    }
    inline pthread_t getPthread(const int tid) {
        return registeredThreads[tid];
    }
    
    void initThread(const int tid) {
        if (MasterRecordMgr::supportsCrashRecovery()) {
            // create mapping between tid and pthread_self for the signal handler
            // and for any thread that neutralizes another
            registeredThreads[tid] = pthread_self();

            // here, we use the fact that errno is defined to be a thread local variable
            errnoThreads[tid] = &errno;
            if (pthread_setspecific(pthreadkey, (void*) (long) tid)) {
                COUTATOMIC("ERROR: failure of pthread_setspecific for tid="<<tid<<std::endl);
            }
            const long __readtid = (long) ((int *) pthread_getspecific(pthreadkey));
            VERBOSE DEBUG COUTATOMICTID("did pthread_setspecific, pthread_getspecific of "<<__readtid<<std::endl);
            assert(__readtid == tid);
        }
    }
    void deinitThread(const int tid) {}
    
    void unblockCrashRecoverySignal() {
        if (MasterRecordMgr::supportsCrashRecovery()) {
            __sync_synchronize();
            sigset_t oldset;
            sigemptyset(&oldset);
            sigaddset(&oldset, neutralizeSignal);
            if (pthread_sigmask(SIG_UNBLOCK, &oldset, NULL)) {
                VERBOSE COUTATOMIC("ERROR UNBLOCKING SIGNAL"<<std::endl);
                exit(-1);
            }
        }
    }
    
    RecoveryMgr(const int numProcesses, const int _neutralizeSignal, MasterRecordMgr * const masterRecordMgr)
            : NUM_PROCESSES(numProcesses) , neutralizeSignal(_neutralizeSignal){
        
        if (MasterRecordMgr::supportsCrashRecovery()) {
            setjmpbuffers = new sigjmp_buf[numProcesses];
            pthread_key_create(&pthreadkey, NULL);
        
            // set up crash recovery signal handling for this process
            memset(&___act, 0, sizeof(___act));
            ___act.sa_sigaction = crashhandler<MasterRecordMgr>; // specify signal handler
            ___act.sa_flags = SA_RESTART | SA_SIGINFO; // restart any interrupted sys calls instead of silently failing
            sigfillset(&___act.sa_mask);               // block signals during handler
            if (sigaction(_neutralizeSignal, &___act, NULL)) {
                COUTATOMIC("ERROR: could not register signal handler for signal "<<_neutralizeSignal<<std::endl);
                assert(false);
                exit(-1);
            } else {
                VERBOSE COUTATOMIC("registered signal "<<_neutralizeSignal<<" for crash recovery"<<std::endl);
            }
            
            // set up shared pointer to this class instance for the signal handler
            ___singleton = (void *) masterRecordMgr;
        }
    }
    ~RecoveryMgr() {
        if (MasterRecordMgr::supportsCrashRecovery()) {
            delete[] setjmpbuffers;
        }
    }
};

#endif	/* RECOVERY_MANAGER_H */

