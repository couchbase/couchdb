/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Filipe Manana  <filipe@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#ifndef _NIF_STL_ALLOCATOR_H
#define _NIF_STL_ALLOCATOR_H

// Some information about STL allocators:
// http://www.cplusplus.com/reference/std/memory/allocator/
//
template<class T> class NifStlAllocator {
public:
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    typedef T*        pointer;
    typedef const T*  const_pointer;
    typedef T&        reference;
    typedef const T&  const_reference;
    typedef T         value_type;

    NifStlAllocator() {}
    NifStlAllocator(const NifStlAllocator&) {}

    pointer allocate(size_type n, const void * = 0) {
        if (n > this->max_size()) {
            throw std::bad_alloc();
        }
        pointer t = static_cast<pointer>(enif_alloc(n * sizeof(value_type)));
        if (!t) {
            throw std::bad_alloc();
        }
        return t;
    }

    void deallocate(void *p, size_type) {
        if (p) {
            enif_free(p);
        }
    }

    pointer address(reference x) const {
        return &x;
    }

    const_pointer address(const_reference x) const {
        return &x;
    }

    void construct(pointer p, const_reference val) {
        new (p) value_type(val);
    }

    void destroy(pointer p) {
        p->~T();
    }

    size_type max_size() const {
        return size_t(-1) / sizeof(value_type);
    }

    template <class U>
    struct rebind {
        typedef NifStlAllocator<U> other;
    };

    template <class U>
    NifStlAllocator(const NifStlAllocator<U>&) {}

    template <class U>
    NifStlAllocator& operator=(const NifStlAllocator<U>&) {
        return *this;
    }
};

template<typename T>
inline bool operator==(const NifStlAllocator<T>&, const NifStlAllocator<T>&) {
    return true;
}

template<typename T>
inline bool operator!=(const NifStlAllocator<T>&, const NifStlAllocator<T>&) {
    return false;
}

#endif
