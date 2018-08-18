/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb
{
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id)
{
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(1); //when have a key, size = 2, so there is no key and size = 1

    auto ss = (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / (sizeof(MappingType));
    SetMaxSize(ss);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const
{
    // replace with your own code
    assert(index >= 0 && index < GetSize());
    return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key)
{
    assert(index >= 0 && index < GetSize());
    array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const
{
    assert(GetSize() > 1);
    for (int i = 0; i < GetSize(); i++)
    {
        if (array[i].second == value)
        {
            return i;
        }
    }
    return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const
{
    assert(index >= 0 && index < GetSize());
    return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const
{
    assert(GetSize() > 1);
    int l = 1, r = GetSize() - 1;
    while (l <= r)
    {
        int mid = (l + r) / 2;
        if (comparator(array[mid].first, key) <= 0)
            l = mid + 1;
        else
            r = mid - 1;
    }
    return array[r].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value)
{
    assert(GetSize() == 1);
    array[0].second = old_value;
    array[1].first = new_key;
    array[1].second = new_value;
    IncreaseSize(1);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value)
{
    assert(GetSize() > 1);
    int index = ValueIndex(old_value);
    for (int i = GetSize() - 1; i > index; i--)
    {
        array[i + 1] = array[i];
    }
    array[index + 1].first = new_key;
    array[index + 1].second = new_value;
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
    int move_size = (GetSize() + 1) / 2;
    recipient->CopyHalfFrom(array + GetSize() - move_size, move_size, buffer_pool_manager);

    for(int i = GetSize() - move_size; i < GetSize(); i++){
        auto *pp = buffer_pool_manager->FetchPage(array[i].second);
        if (pp == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
        auto *bplus = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()), KeyComparator> *>(pp->GetData());
        bplus->SetParentPageId(recipient->GetPageId());

        buffer_pool_manager->UnpinPage(bplus->GetPageId(), true);
    }
    IncreaseSize(-move_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager)
{
    for(int i = 0; i < size; i++){
        array[i] = items[i];
    }
    SetSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index)
{
    assert(index < GetSize());
    for(int i = index + 1; i < GetSize(); i++){
        array[i] = array[i + 1];
    }
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild()
{
    SetSize(1);
    SetPageType(IndexPageType::INVALID_INDEX_PAGE);
    return array[0].second;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager)
{
    auto *parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (parent_page == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
    auto *bplus_parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()), KeyComparator> *>(parent_page->GetData());
    array[0].first = bplus_parent->KeyAt(index_in_parent);
    bplus_parent->Remove(index_in_parent);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);
    for(int i = 0; i < GetSize(); i++){
        auto *pp = buffer_pool_manager->FetchPage(array[i].second);
        if (pp == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
        auto *bplus = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()), KeyComparator> *>(pp->GetData());
        bplus->SetParentPageId(index_in_parent);
        buffer_pool_manager->UnpinPage(bplus->GetPageId(), true);
    }
    SetSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager)
{
    for(int i = 0; i < size; i++){
        array[GetSize() + i] = items[i];
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
    auto *parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (parent_page == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
    auto *bplus_parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()), KeyComparator> *>(parent_page->GetData());
    array[0].first = bplus_parent->KeyAt(bplus_parent->ValueIndex(GetPageId()));
    bplus_parent->SetKeyAt(bplus_parent->ValueIndex(GetPageId()), array[1].first);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);

    recipient->CopyLastFrom(array[0], buffer_pool_manager);
    auto *pp = buffer_pool_manager->FetchPage(array[0].second);
    if (pp == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
    auto *bplus = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()), KeyComparator> *>(pp->GetData());
    bplus->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(GetPageId(), true);
    for(int i = 1; i < GetSize(); i++){
        array[i - 1] = array[i];
    }
    IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager)
{
    array[GetSize()] = pair;
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
    auto *parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (parent_page == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
    auto *bplus_parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()), KeyComparator> *>(parent_page->GetData());
    bplus_parent->SetKeyAt(bplus_parent->ValueIndex(GetPageId()), array[GetSize() - 1].first);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);

    recipient->CopyLastFrom(array[GetSize() - 1], buffer_pool_manager);
    auto *pp = buffer_pool_manager->FetchPage(array[GetSize() - 1].second);
    if (pp == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
    auto *bplus = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()), KeyComparator> *>(pp->GetData());
    bplus->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(GetPageId(), true);
    IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
    for(int i = GetSize(); i > 0; i--){
        array[i] = array[i-1];
    }
    array[0] = pair;
    IncreaseSize(1);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager)
{
    for (int i = 0; i < GetSize(); i++)
    {
        auto *page = buffer_pool_manager->FetchPage(array[i].second);
        if (page == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
        BPlusTreePage *node =
            reinterpret_cast<BPlusTreePage *>(page->GetData());
        queue->push(node);
    }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const
{
    if (GetSize() == 0)
    {
        return "";
    }
    std::ostringstream os;
    if (verbose)
    {
        os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
    }

    int entry = verbose ? 0 : 1;
    int end = GetSize();
    bool first = true;
    while (entry < end)
    {
        if (first)
        {
            first = false;
        }
        else
        {
            os << " ";
        }
        os << std::dec << array[entry].first.ToString();
        if (verbose)
        {
            os << "(" << array[entry].second << ")";
        }
        ++entry;
    }
    return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                     GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                     GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                     GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                     GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                     GenericComparator<64>>;
} // namespace cmudb
