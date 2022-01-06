//
// Created by florian on 18.11.15.
//

#ifndef ART_OPTIMISTICLOCK_COUPLING_N_H
#define ART_OPTIMISTICLOCK_COUPLING_N_H
#include <assert.h>
#include <algorithm>
#include <functional>
#include "N.cpp"
#include "Key.h"

namespace ART_OLC {

    template <class RecordManager>
    class Tree {
    public:
        using LoadKeyFunction = void (*)(TID tid, Key &key);

    private:
        N* root;

        RecordManager* const recmgr;

        TID checkKey(const TID tid, const Key &k) const;

        LoadKeyFunction loadKey;

    public:
        enum class CheckPrefixResult : uint8_t {
            Match,
            NoMatch,
            OptimisticMatch
        };

        enum class CheckPrefixPessimisticResult : uint8_t {
            Match,
            NoMatch,
        };

        enum class PCCompareResults : uint8_t {
            Smaller,
            Equal,
            Bigger,
        };
        enum class PCEqualsResults : uint8_t {
            BothMatch,
            Contained,
            NoMatch
        };
        static CheckPrefixResult checkPrefix(N* n, const Key &k, uint32_t &level);

        static CheckPrefixPessimisticResult checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                   uint8_t &nonMatchingKey,
                                                                   Prefix &nonMatchingPrefix,
                                                                   LoadKeyFunction loadKey, bool &needRestart);

        static PCCompareResults checkPrefixCompare(const N* n, const Key &k, uint8_t fillKey, uint32_t &level, LoadKeyFunction loadKey, bool &needRestart);

        static PCEqualsResults checkPrefixEquals(const N* n, uint32_t &level, const Key &start, const Key &end, LoadKeyFunction loadKey, bool &needRestart);

    public:

        Tree(const int numThreads, LoadKeyFunction loadKey);

        Tree(const Tree &) = delete;

        Tree(Tree &&t) : root(t.root), loadKey(t.loadKey) { }

        ~Tree();

        void cleanup(N* node);

        void initThread(const int threadID);

        void deinitThread(const int threadID);

        TID lookup(const int threadID, const Key &k) const;

        bool insert(const int threadID, const Key &k, TID tid);

        bool remove(const int threadID, const Key &k, TID tid);

        N* alloc(const int threadID, NTypes type);

        N* const getRoot() {
            return root;
        }
    };

    template <class RecordManager>
    Tree<RecordManager>::Tree(const int numThreads, LoadKeyFunction loadKey) : recmgr(new RecordManager(numThreads)), loadKey(loadKey) {
        const int threadID = 0;
        initThread(threadID);
        root = recmgr->template allocate<N256>(threadID);
        root->setPrefix(nullptr, 0);
    }

    template <class RecordManager>
    Tree<RecordManager>::~Tree() {
        initThread(0);
        cleanup(root);
        deinitThread(0);
        delete recmgr;
    }

    template <class RecordManager>
    void Tree<RecordManager>::cleanup(N* node) {
        if (N::isLeaf(node)) {
            return;
        }
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = (N4*)node;
                for (uint32_t i = 0; i < n->getCount(); ++i) {
                    cleanup(n->children[i]);
                }
                recmgr->deallocate(0, n);
                break;
            }
            case NTypes::N16: {
                auto n = (N16*)node;
                for (uint32_t i = 0; i < n->getCount(); ++i) {
                    cleanup(n->children[i]);
                }
                recmgr->deallocate(0, n);
                break;
            }
            case NTypes::N48: {
                auto n = (N48*)node;
                for (unsigned i = 0; i < 256; i++) {
                    if (n->childIndex[i] != N48::emptyMarker) {
                        cleanup(n->children[n->childIndex[i]]);
                    }
                }
                recmgr->deallocate(0, n);
                break;
            }
            case NTypes::N256: {
                auto n = (N256*)node;
                for (unsigned i = 0; i < 256; i++) {
                    if (n->children[i] != nullptr) {
                        cleanup(n->children[i]);
                    }
                }
                recmgr->deallocate(0, n);
                break;
            }
        }
    }

    template <class RecordManager>
    void Tree<RecordManager>::initThread(const int threadID) {
        recmgr->initThread(threadID);
    }

    template <class RecordManager>
    void Tree<RecordManager>::deinitThread(const int threadID) {
        recmgr->deinitThread(threadID);
    }

    template <class RecordManager>
    TID Tree<RecordManager>::lookup(const int threadID, const Key &k) const {
        restart:
        auto guard = recmgr->getGuard(threadID);
        bool needRestart = false;

        N *node;
        N *parentNode = nullptr;
        uint64_t v;
        uint32_t level = 0;
        bool optimisticPrefixMatch = false;

        node = root;
        v = node->readLockOrRestart(needRestart);
        if (needRestart) goto restart;
        while (true) {
            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    node->readUnlockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    return 0;
                case CheckPrefixResult::OptimisticMatch:
                    optimisticPrefixMatch = true;
                    // fallthrough
                case CheckPrefixResult::Match:
                    if (k.getKeyLen() <= level) {
                        return 0;
                    }
                    parentNode = node;
                    node = N::getChild(k[level], parentNode);
                    parentNode->checkOrRestart(v,needRestart);
                    if (needRestart) goto restart;

                    if (node == nullptr) {
                        return 0;
                    }
                    if (N::isLeaf(node)) {
                        parentNode->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;

                        TID tid = N::getLeaf(node);
                        if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
                            return checkKey(tid, k);
                        }
                        return tid;
                    }
                    level++;
            }
            uint64_t nv = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            parentNode->readUnlockOrRestart(v, needRestart);
            if (needRestart) goto restart;
            v = nv;
        }
    }

    template <class RecordManager>
    TID Tree<RecordManager>::checkKey(const TID tid, const Key &k) const {
        // Anubhav: It looks like this extracts the key from the TID (which is the KV pair?)
        //          so, the key and value have to both fit in the TID (64 bits).
        Key kt;
        this->loadKey(tid, kt);
        if (k == kt) {
            return tid;
        }
        return 0;
    }

    template <class RecordManager>
    bool Tree<RecordManager>::insert(const int threadID, const Key &k, TID tid) {
        restart:
        auto guard = recmgr->getGuard(threadID);
        bool needRestart = false;

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint64_t parentVersion = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            uint32_t nextLevel = level;

            uint8_t nonMatchingKey;
            Prefix remainingPrefix;
            auto res = checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                   this->loadKey, needRestart); // increases level
            if (needRestart) goto restart;
            switch (res) {
                case CheckPrefixPessimisticResult::NoMatch: {
                    parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                    if (needRestart) goto restart;

                    node->upgradeToWriteLockOrRestart(v, needRestart);
                    if (needRestart) {
                        parentNode->writeUnlock();
                        goto restart;
                    }
                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    N4* newNode = recmgr->template allocate<N4>(threadID); 
                    newNode->setPrefix(node->getPrefix(), nextLevel - level);

                    // 2)  add node and (tid, *k) as children
                    newNode->insert(k[nextLevel], N::setLeaf(tid)); // Anubhav: Looks like k has to be malloc'd
                    newNode->insert(nonMatchingKey, node);

                    // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
                    N::change(parentNode, parentKey, newNode);
                    parentNode->writeUnlock();

                    // 4) update prefix of node, unlock
                    node->setPrefix(remainingPrefix,
                                    node->getPrefixLength() - ((nextLevel - level) + 1));

                    node->writeUnlock();
                    return true;
                }
                case CheckPrefixPessimisticResult::Match:
                    break;
            }
            level = nextLevel;
            nodeKey = k[level];
            nextNode = N::getChild(nodeKey, node);
            node->checkOrRestart(v,needRestart);
            if (needRestart) goto restart;

            if (nextNode == nullptr) {
                N::insertAndUnlock(threadID, recmgr, node, v, parentNode, parentVersion, parentKey, nodeKey, N::setLeaf(tid), needRestart);
                if (needRestart) goto restart;
                return true;
            }

            if (parentNode != nullptr) {
                parentNode->readUnlockOrRestart(parentVersion, needRestart);
                if (needRestart) goto restart;
            }

            if (N::isLeaf(nextNode)) {
                if (N::getLeaf(nextNode) == tid) {
                    return false;
                }

                node->upgradeToWriteLockOrRestart(v, needRestart);
                if (needRestart) goto restart;

                Key key;
                loadKey(N::getLeaf(nextNode), key);

                level++;
                uint32_t prefixLength = 0;
                // Anubhav: Are key and k null-terminated? If not, isn't this unsafe array access?
                while (key[level + prefixLength] == k[level + prefixLength]) {
                    prefixLength++;
                }

                N4* n4 = recmgr->template allocate<N4>(threadID);
                n4->setPrefix(&k[level], prefixLength);
                n4->insert(k[level + prefixLength], N::setLeaf(tid));
                n4->insert(key[level + prefixLength], nextNode);
                N::change(node, k[level - 1], n4);
                node->writeUnlock();
                return true;
            }
            level++;
            parentVersion = v;
        }
    }

    template <class RecordManager>
    bool Tree<RecordManager>::remove(const int threadID, const Key &k, TID tid) {
        restart:
        auto guard = recmgr->getGuard(threadID);
        bool needRestart = false;

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint64_t parentVersion = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    node->readUnlockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    return false;
                case CheckPrefixResult::OptimisticMatch:
                    // fallthrough
                case CheckPrefixResult::Match: {
                    nodeKey = k[level];
                    nextNode = N::getChild(nodeKey, node);

                    node->checkOrRestart(v, needRestart);
                    if (needRestart) goto restart;

                    if (nextNode == nullptr) {
                        node->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;
                        return false;
                    }
                    if (N::isLeaf(nextNode)) {
                        if (N::getLeaf(nextNode) != tid) {
                            return false;
                        }
                        assert(parentNode == nullptr || node->getCount() != 1);
                        if (node->getCount() == 2 && parentNode != nullptr) {
                            parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                            if (needRestart) goto restart;

                            node->upgradeToWriteLockOrRestart(v, needRestart);
                            if (needRestart) {
                                parentNode->writeUnlock();
                                goto restart;
                            }
                            // 1. check remaining entries
                            N *secondNodeN;
                            uint8_t secondNodeK;
                            std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
                            if (N::isLeaf(secondNodeN)) {
                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);

                                parentNode->writeUnlock();
                                node->writeUnlockObsolete();
                                recmgr->retire(threadID, node);
                            } else {
                                secondNodeN->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    parentNode->writeUnlock();
                                    goto restart;
                                }

                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);
                                parentNode->writeUnlock();

                                secondNodeN->addPrefixBefore(node, secondNodeK);
                                secondNodeN->writeUnlock();

                                node->writeUnlockObsolete();
                                recmgr->retire(threadID, node);
                            }
                        } else {
                            N::removeAndUnlock(threadID, recmgr, node, v, k[level], parentNode, parentVersion, parentKey, needRestart);
                            if (needRestart) goto restart;
                        }
                        return true;
                    }
                    level++;
                    parentVersion = v;
                }
            }
        }
    }

    template <class RecordManager>
    N* Tree<RecordManager>::alloc(const int threadID, NTypes type) {
        switch (type) {
            case NTypes::N4:
                return recmgr->template allocate<N4>(threadID);
            case NTypes::N16:
                return recmgr->template allocate<N16>(threadID);
            case NTypes::N48:
                return recmgr->template allocate<N48>(threadID);
            case NTypes::N256:
                return recmgr->template allocate<N256>(threadID);
        }
    }

    template <class RecordManager>
    inline typename Tree<RecordManager>::CheckPrefixResult Tree<RecordManager>::checkPrefix(N *n, const Key &k, uint32_t &level) {
        if (n->hasPrefix()) {
            if (k.getKeyLen() <= level + n->getPrefixLength()) {
                return CheckPrefixResult::NoMatch;
            }
            for (uint32_t i = 0; i < std::min(n->getPrefixLength(), maxStoredPrefixLength); ++i) {
                if (n->getPrefix()[i] != k[level]) {
                    return CheckPrefixResult::NoMatch;
                }
                ++level;
            }
            if (n->getPrefixLength() > maxStoredPrefixLength) {
                level = level + (n->getPrefixLength() - maxStoredPrefixLength);
                return CheckPrefixResult::OptimisticMatch;
            }
        }
        return CheckPrefixResult::Match;
    }

    template <class RecordManager>
    typename Tree<RecordManager>::CheckPrefixPessimisticResult Tree<RecordManager>::checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                        uint8_t &nonMatchingKey,
                                                                        Prefix &nonMatchingPrefix,
                                                                        LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            uint32_t prevLevel = level;
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return CheckPrefixPessimisticResult::Match;
                    loadKey(anyTID, kt);
                }
                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey != k[level]) {
                    nonMatchingKey = curKey;
                    if (n->getPrefixLength() > maxStoredPrefixLength) {
                        if (i < maxStoredPrefixLength) {
                            auto anyTID = N::getAnyChildTid(n, needRestart);
                            if (needRestart) return CheckPrefixPessimisticResult::Match;
                            loadKey(anyTID, kt);
                        }
                        memcpy(nonMatchingPrefix, &kt[0] + level + 1, std::min((n->getPrefixLength() - (level - prevLevel) - 1),
                                                                           maxStoredPrefixLength));
                    } else {
                        memcpy(nonMatchingPrefix, n->getPrefix() + i + 1, n->getPrefixLength() - i - 1);
                    }
                    return CheckPrefixPessimisticResult::NoMatch;
                }
                ++level;
            }
        }
        return CheckPrefixPessimisticResult::Match;
    }

    template <class RecordManager>
    typename Tree<RecordManager>::PCCompareResults Tree<RecordManager>::checkPrefixCompare(const N *n, const Key &k, uint8_t fillKey, uint32_t &level,
                                                        LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return PCCompareResults::Equal;
                    loadKey(anyTID, kt);
                }
                uint8_t kLevel = (k.getKeyLen() > level) ? k[level] : fillKey;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey < kLevel) {
                    return PCCompareResults::Smaller;
                } else if (curKey > kLevel) {
                    return PCCompareResults::Bigger;
                }
                ++level;
            }
        }
        return PCCompareResults::Equal;
    }

    template <class RecordManager>
    typename Tree<RecordManager>::PCEqualsResults Tree<RecordManager>::checkPrefixEquals(const N *n, uint32_t &level, const Key &start, const Key &end,
                                                      LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return PCEqualsResults::BothMatch;
                    loadKey(anyTID, kt);
                }
                uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey > startLevel && curKey < endLevel) {
                    return PCEqualsResults::Contained;
                } else if (curKey < startLevel || curKey > endLevel) {
                    return PCEqualsResults::NoMatch;
                }
                ++level;
            }
        }
        return PCEqualsResults::BothMatch;
    }
}

#endif //ART_OPTIMISTICLOCK_COUPLING_N_H
