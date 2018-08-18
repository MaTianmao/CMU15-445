/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb
{

template <typename T>
LRUReplacer<T>::LRUReplacer()
{
    head = NULL;
    tail = NULL;
    size = 0;
    metadata.clear();
}

template <typename T>
LRUReplacer<T>::~LRUReplacer()
{
    while (head != NULL)
    {
        LRUlist<T> *next = head->next;
        delete head;
        head = next;
    }
    size = 0;
    metadata.clear();
}

/*
 * Insert value into LRU
 */
template <typename T>
void LRUReplacer<T>::Insert(const T &value)
{
    mtx.lock();
    if (tail == NULL)
    {
        LRUlist<T> *pp = new LRUlist<T>;
        pp->value = value;
        pp->next = NULL;
        pp->prev = NULL;
        head = tail = pp;
        size++;
        metadata[value] = pp;
    }
    else
    {
        if(metadata.count(value)){
            mtx.unlock();
            Erase(value);
            mtx.lock();
        }
        LRUlist<T> *pp = new LRUlist<T>;
        pp->value = value;
        pp->next = NULL;
        pp->prev = tail;
        tail->next = pp;
        tail = pp;
        size++;
        metadata[value] = pp;
    }
    mtx.unlock();
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T>
bool LRUReplacer<T>::Victim(T &value)
{
    if (size == 0)
        return false;
    mtx.lock();
    LRUlist<T> *vic = head;
    value = vic->value;
    head = head->next;
    if(head != NULL) head->prev = NULL;
    size--;
    if(size == 0) tail = NULL;
    delete vic;
    metadata.erase(value);
    mtx.unlock();
    return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T>
bool LRUReplacer<T>::Erase(const T &value)
{
    mtx.lock();
    if(metadata.count(value)){
        LRUlist<T> *pp = metadata[value];
        if(pp == head){
            head = pp->next;
            if(head != NULL) head->prev = NULL;
        }
        else if(pp == tail){
            tail = pp->prev;
            if(tail != NULL) tail->next = NULL;
        }
        else{
            pp->prev->next = pp->next;
            pp->next->prev = pp->prev;
        }
        metadata.erase(value);
        size--;
        delete pp;
        mtx.unlock();
        return true;
    }
    mtx.unlock();
    return false;
}

template <typename T>
size_t LRUReplacer<T>::Size() { return size; }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
