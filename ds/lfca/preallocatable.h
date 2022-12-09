/**
 * Preallocatable is a class which allows subclasses to be preallocated before use and handles distributing references to those elements.
 * Simply extend this class and use `MyClass::New()` in place of `new MyClass()` after preallocating.
 *
 * This uses the curiously recurring template pattern (CRTP) to allow the class to create and return references to derived classes.
 */

#ifndef _PREALLOCATABLE_H
#define _PREALLOCATABLE_H

#include <atomic>
#include <stdexcept>
#include <string>
#include <typeinfo>

template <class T>
class Preallocatable {
private:
    // Meyers' singleton variable declarations to allow initializing static variables

    static bool &_isPreallocated() {
        static bool _val{false};
        return _val;
    };
    static int &_numPreallocated() {
        static int _val{0};
        return _val;
    }

    static T *&_preallocatedElements() {
        static T *_val{nullptr};
        return _val;
    }
    static std::atomic<int> &_currentElementIndex() {
        static std::atomic<int> _val{0};
        return _val;
    }

public:
    /**
     * Preallocates elements for this class.
     *
     * @param numElements
     * The number of elements to preallocate.
     */
    static void Preallocate(int numElements) {
        if (_isPreallocated()) {
            throw std::logic_error(std::string("Cannot preallocate: Class ") + typeid(T).name() + " is already preallocated.");
        }

        _preallocatedElements() = new T[numElements];
        _numPreallocated() = numElements;
        _isPreallocated() = true;
        _currentElementIndex().store(0);
    };

    /**
     * Deallocates all preallocated elements.
     */
    static void Deallocate() {
        delete[] _preallocatedElements();
        _isPreallocated() = false;
    };

    /**
     * Retrieves a preallocated element.
     *
     * @return T*
     * The preallocated element.
     */
    static T *New() {
        if (!_isPreallocated()) {
            // TODO: If no elements have been preallocated, this could act like a standard `new` operator and create an element.
            throw std::logic_error(std::string("Cannot retrieve preallocated element of class ") + typeid(T).name() + ": No elements have been preallocated");
        }

        // Get a node index
        int index = _currentElementIndex().fetch_add(1);

        // Verify that this is a valid index
        if (index >= _numPreallocated()) {
            throw std::out_of_range(std::string("Cannot retrieve preallocated element ") + std::to_string(index) + " of class " + typeid(T).name() + ": The maximum of " + std::to_string(_numPreallocated()) + " elements has been reached.");
        }

        // Return the element
        return &_preallocatedElements()[index];
    };

    /**
     * Retrieves a preallocated element, copying from the other element using the assignment operator.
     * Note that this does *not* use the copy constructor.
     *
     * @param other
     * The element to copy from.
     *
     * @return T*
     * A preallocated element, copied from the other element.
     */
    static T *New(const T &other) {
        T *newElement = New();
        *newElement = other;

        return newElement;
    }
};

#endif /* _PREALLOCATABLE_H */
