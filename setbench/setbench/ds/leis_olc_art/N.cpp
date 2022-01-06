#include <assert.h>
#include <algorithm>
#include <iostream>

#include "N.h"
#include "N4.cpp"
#include "N16.cpp"
#include "N48.cpp"
#include "N256.cpp"

namespace ART_OLC {

    void N::setType(NTypes type) {
        typeVersionLockObsolete.fetch_add(convertTypeToVersion(type));
    }

    uint64_t N::convertTypeToVersion(NTypes type) {
        return (static_cast<uint64_t>(type) << 62);
    }

    NTypes N::getType() const {
        return static_cast<NTypes>(typeVersionLockObsolete.load(std::memory_order_relaxed) >> 62);
    }

    void N::writeLockOrRestart(bool &needRestart) {

        uint64_t version;
        version = readLockOrRestart(needRestart);
        if (needRestart) return;

        upgradeToWriteLockOrRestart(version, needRestart);
        if (needRestart) return;
    }

    void N::upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
        if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
            version = version + 0b10;
        } else {
            needRestart = true;
        }
    }

    void N::writeUnlock() {
        typeVersionLockObsolete.fetch_add(0b10);
    }

    N *N::getAnyChild(const N *node) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                return n->getAnyChild();
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    bool N::change(N *node, uint8_t key, N *val) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                return n->change(key, val);
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                return n->change(key, val);
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                return n->change(key, val);
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                return n->change(key, val);
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    template<typename curN, typename biggerN, typename RecordManager>
    void N::insertGrow(const int threadID, RecordManager* const recmgr, curN *n, uint64_t v, N *parentNode, uint64_t parentVersion, uint8_t keyParent, uint8_t key, N *val, bool &needRestart) {
        if (!n->isFull()) {
            if (parentNode != nullptr) {
                parentNode->readUnlockOrRestart(parentVersion, needRestart);
                if (needRestart) return;
            }
            n->upgradeToWriteLockOrRestart(v, needRestart);
            if (needRestart) return;
            n->insert(key, val);
            n->writeUnlock();
            return;
        }

        parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
        if (needRestart) return;

        n->upgradeToWriteLockOrRestart(v, needRestart);
        if (needRestart) {
            parentNode->writeUnlock();
            return;
        }

        biggerN* nBig = recmgr->template allocate<biggerN>(threadID);
        nBig->setPrefix(n->getPrefix(), n->getPrefixLength());
        n->copyTo(nBig);
        nBig->insert(key, val);

        N::change(parentNode, keyParent, nBig);

        n->writeUnlockObsolete();
        recmgr->retire(threadID, n);
        parentNode->writeUnlock();
    }

    template <typename RecordManager>
    void N::insertAndUnlock(const int threadID, RecordManager* const recmgr, N *node, uint64_t v, N *parentNode, uint64_t parentVersion, uint8_t keyParent, uint8_t key, N *val, bool &needRestart) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                insertGrow<N4, N16>(threadID, recmgr, n, v, parentNode, parentVersion, keyParent, key, val, needRestart);
                break;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                insertGrow<N16, N48>(threadID, recmgr, n, v, parentNode, parentVersion, keyParent, key, val, needRestart);
                break;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                insertGrow<N48, N256>(threadID, recmgr, n, v, parentNode, parentVersion, keyParent, key, val, needRestart);
                break;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                insertGrow<N256, N256>(threadID, recmgr, n, v, parentNode, parentVersion, keyParent, key, val, needRestart);
                break;
            }
        }
    }

    inline N *N::getChild(const uint8_t k, const N *node) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                return n->getChild(k);
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                return n->getChild(k);
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                return n->getChild(k);
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                return n->getChild(k);
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    template<typename curN, typename smallerN, typename RecordManager>
    void N::removeAndShrink(const int threadID, RecordManager* const recmgr, curN *n, uint64_t v, N *parentNode, uint64_t parentVersion, uint8_t keyParent, uint8_t key, bool &needRestart) {
        if (!n->isUnderfull() || parentNode == nullptr) {
            if (parentNode != nullptr) {
                parentNode->readUnlockOrRestart(parentVersion, needRestart);
                if (needRestart) return;
            }
            n->upgradeToWriteLockOrRestart(v, needRestart);
            if (needRestart) return;

            n->remove(key);
            n->writeUnlock();
            return;
        }
        parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
        if (needRestart) return;

        n->upgradeToWriteLockOrRestart(v, needRestart);
        if (needRestart) {
            parentNode->writeUnlock();
            return;
        }

        smallerN* nSmall = recmgr->template allocate<smallerN>(threadID);
        nSmall->setPrefix(n->getPrefix(), n->getPrefixLength());
        n->copyTo(nSmall);
        nSmall->remove(key);
        N::change(parentNode, keyParent, nSmall);

        n->writeUnlockObsolete();
        recmgr->retire(threadID, n);
        parentNode->writeUnlock();
    }

    template <typename RecordManager>
    void N::removeAndUnlock(const int threadID, RecordManager* const recmgr, N *node, uint64_t v, uint8_t key, N *parentNode, uint64_t parentVersion, uint8_t keyParent, bool &needRestart) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                removeAndShrink<N4, N4, RecordManager>(threadID, recmgr, n, v, parentNode, parentVersion, keyParent, key, needRestart);
                break;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                removeAndShrink<N16, N4, RecordManager>(threadID, recmgr, n, v, parentNode, parentVersion, keyParent, key, needRestart);
                break;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                removeAndShrink<N48, N16, RecordManager>(threadID, recmgr, n, v, parentNode, parentVersion, keyParent, key, needRestart);
                break;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                return removeAndShrink<N256, N48, RecordManager>(threadID, recmgr, n, v, parentNode, parentVersion, keyParent, key, needRestart);
                break;
            }
        }
    }

    bool N::isLocked(uint64_t version) const {
        return ((version & 0b10) == 0b10);
    }

    uint64_t N::readLockOrRestart(bool &needRestart) const {
        uint64_t version;
        version = typeVersionLockObsolete.load();
/*        do {
            version = typeVersionLockObsolete.load();
        } while (isLocked(version));*/
        if (isLocked(version) || isObsolete(version)) {
            needRestart = true;
        }
        return version;
        //uint64_t version;
        //while (isLocked(version)) _mm_pause();
        //return version;
    }

    bool N::isObsolete(uint64_t version) {
        return (version & 1) == 1;
    }

    void N::checkOrRestart(uint64_t startRead, bool &needRestart) const {
        readUnlockOrRestart(startRead, needRestart);
    }

    void N::readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
        needRestart = (startRead != typeVersionLockObsolete.load());
    }

    uint32_t N::getPrefixLength() const {
        return prefixCount;
    }

    bool N::hasPrefix() const {
        return prefixCount > 0;
    }

    uint32_t N::getCount() const {
        return count;
    }

    const uint8_t *N::getPrefix() const {
        return prefix;
    }

    void N::setPrefix(const uint8_t *prefix, uint32_t length) {
        if (length > 0) {
            memcpy(this->prefix, prefix, std::min(length, maxStoredPrefixLength));
            prefixCount = length;
        } else {
            prefixCount = 0;
        }
    }

    void N::addPrefixBefore(N *node, uint8_t key) {
        uint32_t prefixCopyCount = std::min(maxStoredPrefixLength, node->getPrefixLength() + 1);
        memmove(this->prefix + prefixCopyCount, this->prefix,
                std::min(this->getPrefixLength(), maxStoredPrefixLength - prefixCopyCount));
        memcpy(this->prefix, node->prefix, std::min(prefixCopyCount, node->getPrefixLength()));
        if (node->getPrefixLength() < maxStoredPrefixLength) {
            this->prefix[prefixCopyCount - 1] = key;
        }
        this->prefixCount += node->getPrefixLength() + 1;
    }


    bool N::isLeaf(const N *n) {
        return (reinterpret_cast<uint64_t>(n) & (static_cast<uint64_t>(1) << 63)) == (static_cast<uint64_t>(1) << 63);
    }

    N *N::setLeaf(TID tid) {
        return reinterpret_cast<N *>(tid | (static_cast<uint64_t>(1) << 63));
    }

    TID N::getLeaf(const N *n) {
        return (reinterpret_cast<uint64_t>(n) & ((static_cast<uint64_t>(1) << 63) - 1));
    }

    std::tuple<N *, uint8_t> N::getSecondChild(N *node, const uint8_t key) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                return n->getSecondChild(key);
            }
            default: {
                assert(false);
                __builtin_unreachable();
            }
        }
    }

    TID N::getAnyChildTid(const N *n, bool &needRestart) {
        const N *nextNode = n;

        while (true) {
            const N *node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) return 0;

            nextNode = getAnyChild(node);
            node->readUnlockOrRestart(v, needRestart);
            if (needRestart) return 0;

            assert(nextNode != nullptr);
            if (isLeaf(nextNode)) {
                return getLeaf(nextNode);
            }
        }
    }

    uint64_t N::getChildren(const N *node, uint8_t start, uint8_t end, std::tuple<uint8_t, N *> children[],
                        uint32_t &childrenCount) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                return n->getChildren(start, end, children, childrenCount);
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                return n->getChildren(start, end, children, childrenCount);
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                return n->getChildren(start, end, children, childrenCount);
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                return n->getChildren(start, end, children, childrenCount);
            }
        }
        assert(false);
        __builtin_unreachable();
    }
}