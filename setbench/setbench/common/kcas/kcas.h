#pragma once

#define CASWORD_BITS_TYPE casword_t

#ifndef likely
#define likely(x)       __builtin_expect((x),1)
#endif
#ifndef unlikely
#define unlikely(x)     __builtin_expect((x),0)
#endif

template <typename T>
struct casword {
public:
    CASWORD_BITS_TYPE volatile bits;
public:
    casword();

    T setInitVal(T other);

    operator T();

    T operator->();

    T getValue();

    casword_t getValueUnsafe(bool &isPtr);

    void addToDescriptor(T oldVal, T newVal);
};

#if defined KCAS_LOCKFREE
#include "kcas_reuse_impl.h"
#elif defined KCAS_HTM
#include "kcas_reuse_htm_impl.h"
#elif defined KCAS_HTM_FULL
#include "kcas_reuse_htm_full_impl.h"
#elif defined KCAS_VALIDATE
#include "kcas_validate.h"
#elif defined KCAS_VALIDATE_HTM
#include "kcas_validate_htm.h"
#else
#error must define one of KCAS_LOCKFREE KCAS_HTM KCAS_HTM_FULL
#endif

namespace kcas {
    #if defined KCAS_LOCKFREE
    KCASLockFree<MAX_KCAS> instance;
    #elif defined KCAS_HTM
    KCASHTM<MAX_KCAS> instance;
    #elif defined KCAS_HTM_FULL
    KCASHTM_FULL<MAX_KCAS> instance;
    #elif defined KCAS_VALIDATE
    KCASValidate<MAX_KCAS> instance;
    #elif defined KCAS_VALIDATE_HTM
    KCASValidateHTM<MAX_KCAS> instance;
    #endif

    void writeInitPtr(casword_t volatile * addr, casword_t const newval) {
        return instance.writeInitPtr(addr, newval);
    }

    void writeInitVal(casword_t volatile * addr, casword_t const newval) {
        return instance.writeInitVal(addr, newval);
    }

    casword_t readPtr(casword_t volatile * addr) {
        return instance.readPtr(addr);
    }

    casword_t readVal(casword_t volatile * addr) {
        return instance.readVal(addr);
    }

    bool execute() {
        return instance.execute();
    }

    inline kcasptr_t getDescriptor() {
        return instance.getDescriptor();
    }

    bool start() {
        instance.start();
        return true;
    }

    template<typename T>
    void add(casword<T> * caswordptr, T oldVal, T newVal) {
        return instance.add(caswordptr, oldVal, newVal);
    }

    template<typename T, typename... Args>
    void add(casword<T> * caswordptr, T oldVal, T newVal, Args... args) {
        instance.add(caswordptr, oldVal, newVal, args...);
    }

    #if defined KCAS_VALIDATE || defined KCAS_VALIDATE_HTM

    inline bool validate()
    {
        return instance.validate();
    }


    inline bool validateAndExecute()
    {
        return instance.validateAndExecute();
    }

    template <typename NodePtrType>
    inline casword_t visit(NodePtrType node)
    {
        assert(node != NULL);
        return instance.visit(node);
    }
    #endif


};

#include "casword.h"