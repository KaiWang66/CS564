
/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{


// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
/**
 * BTreeIndex Constructor.
   * Check to see if the corresponding index file exists. If so, open the file.
   * If not, create it and insert entries for every tuple in the base relation using FileScan class.
 *
 * @param relationName        Name of file.
 * @param outIndexName        Return the name of index file.
 * @param bufMgrIn						Buffer Manager Instance
 * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
 * @param attrType						Datatype of attribute over which index is built
 * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values
 *      in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through
 *      constructor parameters.
 */
BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    // construct index name
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    outIndexName = idxStr.str();

    // set private fields
    this->attrByteOffset = attrByteOffset;
    this->attributeType = attrType;
    this->scanExecuting = false;
    // check if the index file exists, if yes, open the file
    // if no, create a new index file
    if (BlobFile::exists(outIndexName)) {
//        file = new BlobFile(outIndexName, false);
//        checkMetaValid(relationName, attrByteOffset, attrType);
//        this->headerPageNum = 1;
        File::remove(outIndexName);
    }

    file = new BlobFile(outIndexName, true);
    // create root page
    bufMgr = bufMgrIn;
    PageId root_id;
    NonLeafNodeInt* root = createNonLeafNode(root_id);
    root->level = 1;
    bufMgrIn->unPinPage(file, root_id, true);

    // create metaInfo page
    Page* meta_page;
    PageId meta_pageId;
    bufMgrIn->allocPage(file, meta_pageId, meta_page);
    IndexMetaInfo *meta_info = (IndexMetaInfo*)meta_page;
    meta_info->attrByteOffset = attrByteOffset;
    meta_info->attrType = attrType;
    relationName.copy(meta_info->relationName, 20, 0);
    meta_info->rootPageNo = root_id;
    bufMgrIn->unPinPage(file, meta_pageId, true);
    this->rootPageNum = root_id;
    this->headerPageNum = meta_pageId;

    // scan the relation
    FileScan fscan(relationName, bufMgrIn);
    try {
        RecordId scanRid;
        while(1) {
            fscan.scanNext(scanRid);
            std::string recordStr = fscan.getRecord();
            const char *record = recordStr.c_str();
            int key = *((int *)(record + attrByteOffset));
            insertEntry(&key, scanRid);
        }
    }
    catch(EndOfFileException e) {
        // The stream will be automatically closed when the last File object is out of scope;
        // no explicit close command is necessary.
    }

}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
/**
 * BTreeIndex Destructor.
 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
 * and delete file instance thereby closing the index file.
 * Destructor should not throw any exceptions. All exceptions should be caught in here itself.
 */
BTreeIndex::~BTreeIndex()
{
    if (scanExecuting) endScan();
    bufMgr->flushFile(file);
    delete file;
}

/**
 * Assume file exists, Check is the metaInfo page is consistent with given info
 * @param relationName
 * @param attrByteOffset
 * @param attrType
 */
const void BTreeIndex::checkMetaValid(const std::string & relationName, int attrByteOffset, Datatype attrType) {
    Page* meta_page = (Page*)file;
    IndexMetaInfo *meta_info = (IndexMetaInfo*)meta_page;
    if (relationName.compare(0, 20, meta_info->relationName) != 0)
        throw BadIndexInfoException ("relationName doesn't match");
    if (attrByteOffset != meta_info->attrByteOffset)
        throw BadIndexInfoException("attrByteOffset doesn't match");
    if (attrType != meta_info->attrType)
        throw BadIndexInfoException("attrType doesn't match");
    this->rootPageNum = meta_info->rootPageNo;
    bufMgr->unPinPage(file, 1, false);
}

/**
 * Create a NonLeaf Node
 * @param return pageId of the NonLeafNode
 * @return a pointer of allocated Page
 */
NonLeafNodeInt* BTreeIndex::createNonLeafNode(PageId &pageId) {
    Page* page;
    bufMgr->allocPage(file, pageId, page);
    memset(page, 0, Page::SIZE);
    return (NonLeafNodeInt*) page;
}


/**
 * Create a Leaf Node
 * @param return pageId of the LeafNode
 * @return a pointer of allocated Page
 */
LeafNodeInt* BTreeIndex::createLeafNode(PageId &pageId) {
    Page* page;
    bufMgr->allocPage(file, pageId, page);
    memset(page, 0, Page::SIZE);
    return (LeafNodeInt*) page;
}

/**
 * This function is called when we need to split the root node
 * Create a new root node with given key, assuming the new node only has one node
 * @param popKey    the popped up key, this will be the only key insert to the root
 * @param left      left pid
 * @param right     right pid
 */
const void BTreeIndex::createRootNode(int popKey, PageId left, PageId right) {
    PageId root_id;
    NonLeafNodeInt* newRoot = createNonLeafNode(root_id);
    newRoot->keyArray[0] = popKey;
    newRoot->pageNoArray[0] = left;
    newRoot->pageNoArray[1] = right;
    newRoot->size = 1;
    newRoot->level = 0;
    bufMgr->unPinPage(file, root_id, true);
    rootPageNum = root_id;
    changeRootPageNumInMetaData();
}

/**
 * change the root page number in meta data to make it consistence with the private field rootPageNum
 */
const void BTreeIndex::changeRootPageNumInMetaData() {
    Page* metaPage;
    bufMgr->readPage(file, headerPageNum, metaPage);
    IndexMetaInfo* metaInfo = (IndexMetaInfo*)metaPage;
    metaInfo->rootPageNo = rootPageNum;
    bufMgr->unPinPage(file, headerPageNum, true);
}

/**
 * insert an <key, pid> entry to a nonLeafNode, assume no split
 * @param nonLeafNode       pointer to nonLeafNode
 * @param key               key to be inserted
 * @param pageNo            pid to be inserted
 * @param index             the insertion index
 */
const void BTreeIndex::insertToNonLeafNode(NonLeafNodeInt* nonLeafNode, int key, PageId pageNo, int index) {
    for (int i = nonLeafNode->size; i > index; i-- ) {
        nonLeafNode->keyArray[i] = nonLeafNode->keyArray[i - 1];
        nonLeafNode->pageNoArray[i + 1] = nonLeafNode->pageNoArray[i];
    }
    nonLeafNode->keyArray[index] = key;
    nonLeafNode->pageNoArray[index + 1] = pageNo;
    nonLeafNode->size = nonLeafNode->size + 1;
}

/**
 * insert an <key, rid> entry to a leafNode, assume no split
 * @param leafNode      pointer to the LeafNode
 * @param key           key to be inserted
 * @param rid           pid to be inserted
 * @param index         the insertion index
 */
const void BTreeIndex::insertToLeafNode(LeafNodeInt* leafNode, int key, RecordId rid, int index) {
    for (int i = leafNode->size; i > index; i--) {
        leafNode->keyArray[i] = leafNode->keyArray[i - 1];
        leafNode->ridArray[i] = leafNode->ridArray[i - 1];
    }
    leafNode->ridArray[index] = rid;
    leafNode->keyArray[index] = key;
    leafNode->size = leafNode->size + 1;
}

/**
 * Find an index to insert a key.
 * Here, we use a modified Binary search to find the smallest element that is larger than the key
 * if all elements are smaller than the key, return size
 * @param keyArray      the array that the key will be inserted
 * @param key
 * @param size          size of the array
 * @return the index of insertion
 */
int BTreeIndex::findIndexToInsert(int* keyArray, int key, int size) {
    if (size == 0) { return 0; }
    int left = 0;
    int right = size - 1;
    if (*(keyArray + right) <= key) {
        return right + 1;
    }
    while (left < right - 1) {
        int mid = left + (right - left) / 2;
        if (*(keyArray + mid) <= key) {
            left = mid + 1;
        } else if (*(keyArray + mid) > key) {
            right = mid;
        }
    }
    // post-processing
    if (*(keyArray + left) > key) {
        return left;
    } else {
        return right;
    }
}

/**
 * Split a nonLeaf node into two by copying half of the record of the old node into the new node
 * Assume the midIndex is the index that need to be popped up into the next higher level
 * @param oldNonLeaf
 * @param newNonLeaf
 * @param midIndex          an index within old nonLeaf node, copy will begin from midIndex + 1
 * @param size              size of the old nonLeaf Node, i.e. oldNonLeaf->size
 */
const void BTreeIndex::splitNonLeafHelper(NonLeafNodeInt* oldNonLeaf, NonLeafNodeInt* newNonLeaf,
        int midIndex, int size) {
    for (int i = midIndex + 1; i < size; i++) {
        newNonLeaf->keyArray[i - midIndex - 1] = oldNonLeaf->keyArray[i];
        newNonLeaf->pageNoArray[i - midIndex - 1] = oldNonLeaf->pageNoArray[i];
    }
    newNonLeaf->pageNoArray[size - midIndex - 1] = oldNonLeaf->pageNoArray[size];
    oldNonLeaf->size = midIndex;
    newNonLeaf->size = size - midIndex - 1;
}

/**
 * This function will be called when a nonLeaf node is full and an entry is need to be inserted
 * The entry that needs to be inserted is <key, pid>
 * Pop up an entry <popKey, popPid> to the next higher level
 * @param nonLeafNode
 * @param nonLeafPid        the pid of the current nonLeaf node
 * @param key
 * @param pid
 * @param popKey            return the popKey
 * @param popPid            return the popPid
 */
const void BTreeIndex::insertAndSplitNonLeaf(NonLeafNodeInt* nonLeafNode, PageId nonLeafPid,
        int key, PageId pid, int &popKey, PageId &popPid) {
    PageId newNonLeafId;
    NonLeafNodeInt* newNonLeaf = createNonLeafNode(newNonLeafId);
    // level of two nonLeafNode should be the same
    newNonLeaf->level = nonLeafNode->level;
    int insertionIndex = findIndexToInsert(nonLeafNode->keyArray, key, nonLeafNode->size);
    int midIndex = (nonLeafNode->size + 1) / 2;
    popKey = nonLeafNode->keyArray[midIndex];
    splitNonLeafHelper(nonLeafNode, newNonLeaf, midIndex, nonLeafNode->size);
    if (insertionIndex == midIndex) {
        insertToNonLeafNode(newNonLeaf, popKey, pid, findIndexToInsert(newNonLeaf->keyArray, popKey,
                newNonLeaf->size));
        popKey = key;
    } else if (insertionIndex < midIndex) {
        insertToNonLeafNode(nonLeafNode, key, pid, insertionIndex);
    } else {
        insertToNonLeafNode(newNonLeaf, key, pid, findIndexToInsert(newNonLeaf->keyArray, key, newNonLeaf->size));
    }
    popPid = newNonLeafId;
    bufMgr->unPinPage(file, nonLeafPid, true);
    bufMgr->unPinPage(file, newNonLeafId, true);
}

/**
 * Split a leaf node into two by copying half of the record of the old node into the new node
 * start copying from the startIndex(inclusive)
 * @param oldLeaf
 * @param newLeaf
 * @param startIndex    an index within the range of the old leaf node
 * @param size          size of the old leaf node, i.e. oldLeaf->size
 */
const void BTreeIndex::splitLeafHelper(LeafNodeInt* oldLeaf, LeafNodeInt* newLeaf, int startIndex, int size) {
    for (int i = startIndex; i < size; i++) {
        newLeaf->keyArray[i - startIndex] = oldLeaf->keyArray[i];
        newLeaf->ridArray[i - startIndex] = oldLeaf->ridArray[i];
    }
    oldLeaf->size = startIndex;
    newLeaf->size = size - startIndex;
}

/**
 * This function will be called when a leaf node is full and an entry is need to be inserted
 * The entry that needs to be inserted is <key, rid>
 * Pop up an entry <popKey, popPid> to the next higher level
 * @param leafNode
 * @param leafPid       the pid of the current leaf node
 * @param key
 * @param rid
 * @param popKey        return the popKey
 * @param popPid        return the popPid
 */
const void BTreeIndex::insertAndSplitLeaf(LeafNodeInt* leafNode, PageId leafPid, int key, RecordId rid,
        int &popKey, PageId &popPid) {
    PageId newLeafId;
    LeafNodeInt* newLeaf = createLeafNode(newLeafId);
    newLeaf->rightSibPageNo = leafNode->rightSibPageNo;
    leafNode->rightSibPageNo = newLeafId;
    int insertionIndex = findIndexToInsert(leafNode->keyArray, key, leafNode->size);
    int midIndex = (leafNode->size + 1) / 2;
    splitLeafHelper(leafNode, newLeaf, midIndex, leafNode->size);
    if (insertionIndex < midIndex) {
        insertToLeafNode(leafNode, key, rid, insertionIndex);
    } else {
        insertToLeafNode(newLeaf, key, rid, findIndexToInsert(newLeaf->keyArray, key, newLeaf->size));
    }
    popKey = newLeaf->keyArray[0];
    popPid = newLeafId;
    bufMgr->unPinPage(file, leafPid, true);
    bufMgr->unPinPage(file, newLeafId, true);
}

/**
 * Check is split is needed
 * @param nonLeafNode
 * @return                  1 if needed, -1 otherwise
 */
int BTreeIndex::checkSplitNonLeaf(NonLeafNodeInt* nonLeafNode) {
    return (nonLeafNode->size + 1 > INTARRAYNONLEAFSIZE) ? 1 : -1;
}

/**
 * Check is split is needed
 * @param leafNode
 * @return                  1 if needed, -1 otherwise
 */
int BTreeIndex::checkSplitLeaf(LeafNodeInt* leafNode) {
    return (leafNode->size + 1 > INTARRAYLEAFSIZE) ? 1 : -1;
}

/**
 * This function will be called with a popped up entry <popKey, popPid>
 * Pop this entry to the next higher level
 * @param popKey
 * @param popPid
 * @param pathPid           pointer to the reverse path of pid, current node has *pathPid, its father node has pid
 *                      of *(path - 1)
 */
const void BTreeIndex::popEntryToNonLeaf(int popKey, PageId popPid, PageId* pathPid) {
    Page* upperNonLeafPage;
    bufMgr->readPage(file, *(pathPid - 1), upperNonLeafPage);
    insertEntryToNonLeaf((NonLeafNodeInt*)upperNonLeafPage, popKey, popPid, pathPid - 1);
}

/**
 * Insert an entry<key, pid> to a nonLeaf node, split may perform recursively
 * @param nonLeafNode
 * @param key
 * @param pageId
 * @param pathPid           pointer to the reverse path of pid, current node has *pathPid, its father node has pid
 *                      of *(path - 1)
 */
const void BTreeIndex::insertEntryToNonLeaf(NonLeafNodeInt* nonLeafNode, int key, PageId pageId, PageId* pathPid) {
    if (checkSplitNonLeaf(nonLeafNode) < 0) {
        insertToNonLeafNode(nonLeafNode, key, pageId, findIndexToInsert(nonLeafNode->keyArray, key,
                nonLeafNode->size));
        bufMgr->unPinPage(file, *pathPid, true);
        return;
    }

    int popKey;
    PageId popPid;
    insertAndSplitNonLeaf(nonLeafNode, *pathPid, key, pageId, popKey, popPid);

    if (*pathPid == rootPageNum) {
        createRootNode(popKey, rootPageNum, popPid);
        return;
    }
    // insert entry to non-leaf recursively
    popEntryToNonLeaf(popKey, popPid, pathPid);
}

/**
 * Insert an entry<key, rid> to a leaf node, split may perform recursively
 * @param leafNode
 * @param key
 * @param recordId
 * @param pathPid           pointer to the reverse path of pid, current node has *pathPid, its father node has pid
 *                      of *(path - 1)
 */
const void BTreeIndex::insertEntryToLeaf(LeafNodeInt* leafNode, int key, RecordId recordId, PageId* pathPid) {
    if (checkSplitLeaf(leafNode) < 0) {
        insertToLeafNode(leafNode, key, recordId, findIndexToInsert(leafNode->keyArray, key, leafNode->size));
        bufMgr->unPinPage(file, *pathPid, true);
        return;
    }
    int popKey;
    PageId popPid;
    insertAndSplitLeaf(leafNode, *pathPid, key, recordId, popKey, popPid);
    popEntryToNonLeaf(popKey, popPid, pathPid);
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
/**
   * Insert a new entry using the pair <value,rid>.
   * Start from root to recursively find out the leaf to insert the entry in.
   * The insertion may cause splitting of leaf node.
   * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn
   *    get split.
   * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs
   *    to be changed accordingly.
   * Make sure to unpin pages as soon as you can.
 * @param key			Key to insert, pointer to integer/double/char string
 * @param rid			Record ID of a record whose entry is getting inserted into the index.
  **/
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    int key_ = *((int*)key);

    PageId pathId[10];
    pathId[0] = rootPageNum;
    int size = 1;
    bool reachLeave = false;
    for (int i = 1; i < 10 && !reachLeave; i++) {
        size++;
        PageId previousId = pathId[i-1];
        Page* previousPage;
        bufMgr->readPage(file, previousId, previousPage);
        NonLeafNodeInt* previousNode = (NonLeafNodeInt*)previousPage;

        if (previousNode->level == 1) {
            reachLeave = true;
        }

        // base case: root is empty
        if (i == 1 && previousNode->size == 0) {
            PageId leftId;
            PageId rightId;
            LeafNodeInt* left = createLeafNode(leftId);
            LeafNodeInt* right = createLeafNode(rightId);
            left->rightSibPageNo = rightId;
            right->rightSibPageNo = 0;
            pathId[1] = rightId;
            previousNode->keyArray[0] = key_;
            previousNode->pageNoArray[0] = leftId;
            previousNode->pageNoArray[1] = rightId;
            previousNode->size = 1;
            insertToLeafNode(right, key_, rid, 0);
            right->size = 1;
            bufMgr->unPinPage(file, leftId, true);
            bufMgr->unPinPage(file, previousId, true);
            bufMgr->unPinPage(file, rightId, true);
            return;
        }

        // find next level
        if (key_ < previousNode->keyArray[0]) {
            pathId[i] = previousNode->pageNoArray[0];
        } else if (key_ > previousNode->keyArray[previousNode->size - 1]) {
            pathId[i] = previousNode->pageNoArray[previousNode->size];
        } else {
            for (int j = 1; j < previousNode->size; j++) {
                if (key_ > previousNode->keyArray[j - 1] && key_ < previousNode->keyArray[j]) {
                    pathId[i] = previousNode->pageNoArray[j];
                }
            }
        }
        bufMgr->unPinPage(file, previousId, false);
    }
    Page* leafPage;
    bufMgr->readPage(file, pathId[size - 1], leafPage);
    LeafNodeInt* leaf =  (LeafNodeInt*)leafPage;
    insertEntryToLeaf(leaf, key_, rid, pathId + size - 1);
}

/**
 * Assume currentPageData might contain next valid entry, find the first valid entry that is within the range
 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
 */
const void BTreeIndex::findNextEntryHelper() {
    // assume currentPageData might contain next valid entry
    bufMgr->readPage(file, currentPageNum, currentPageData);
    LeafNodeInt* leaf = (LeafNodeInt*)currentPageData;
    nextEntry = findIndexToInsert(leaf->keyArray, lowValInt, leaf->size);
    if (nextEntry < leaf->size) {
        return;
    }
    if (leaf->rightSibPageNo == 0) {
        throw NoSuchKeyFoundException();
    }
    PageId next = leaf->rightSibPageNo;
    bufMgr->unPinPage(file, currentPageNum, false);
    currentPageNum = next;
    findNextEntryHelper();
}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
/**
   * Begin a filtered scan of the index.  For instance, if the method is called
   * using ("a",GT,"d",LTE) then we should seek all entries with a value
   * greater than "a" and less than or equal to "d".
   * If another scan is already executing, that needs to be ended here.
   * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
   * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
 * @param lowVal	Low value of range, pointer to integer / double / char string
 * @param lowOp		Low operator (GT/GTE)
 * @param highVal	High value of range, pointer to integer / double / char string
 * @param highOp	High operator (LT/LTE)
 * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
 * @throws  BadScanrangeException If lowVal > highval
   * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
  **/
const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    if (scanExecuting) {
        endScan();
    }
    scanExecuting = true;
    // check error
    if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)) {
        throw BadOpcodesException();
    }

    // set up scanning fields
    lowOp = lowOpParm;
    highOp = highOpParm;
    lowValInt = *((int*)lowValParm);
    highValInt = *((int*)highValParm);

    if (lowValInt > highValInt) {
        throw BadScanrangeException();
    }

    int low = (lowOp == GTE) ? lowValInt - 1 : lowValInt;
    int high = (highOp == LTE) ? highValInt + 1 : highValInt;
    lowValInt = low;
    highValInt = high;
    if (low >= high - 1) {
        throw NoSuchKeyFoundException();
    }

    PageId previousPageId = rootPageNum;
    Page* previousPage;
    bool reachLeave = false;
        for (int i = 0; i < 10 && !reachLeave; i++) {
        bufMgr->readPage(file, previousPageId, previousPage);
        NonLeafNodeInt* previousNode = (NonLeafNodeInt*)previousPage;
        reachLeave = (previousNode->level == 1);
        // base case
        if (previousNode->size == 0) {
            throw NoSuchKeyFoundException();
        }
        if (lowValInt < previousNode->keyArray[0]) {
            currentPageNum = previousNode->pageNoArray[0];
        } else if (lowValInt >= previousNode->keyArray[previousNode->size - 1]) {
            currentPageNum = previousNode->pageNoArray[previousNode->size];
        } else {
            for (int j = 1; j < previousNode->size; j++) {
                if (lowValInt >= previousNode->keyArray[j - 1] && lowValInt < previousNode->keyArray[j]) {
                    currentPageNum = previousNode->pageNoArray[j];
                }
            }
        }

        bufMgr->unPinPage(file, previousPageId, false);
        previousPageId = currentPageNum;
    }

    // currentPageNum is the leaf page we need
    findNextEntryHelper();
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------
/**
   * Fetch the record id of the next index entry that matches the scan.
   * Return the next record from current page being scanned. If current page has been scanned to its entirety,
   * move on to the right sibling of current page, if any exists, to start scanning that page.
   * Make sure to unpin any pages that are no longer required.
 * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
   * @throws ScanNotInitializedException If no scan has been initialized.
   * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
  **/
const void BTreeIndex::scanNext(RecordId& outRid) 
{
    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }
    LeafNodeInt* leaf = (LeafNodeInt*)currentPageData;
    if (nextEntry >= leaf->size) {
        PageId next = leaf->rightSibPageNo;
        if (next == 0) {
            bufMgr->unPinPage(file, currentPageNum, false);
            throw IndexScanCompletedException();
        }
        bufMgr->unPinPage(file, currentPageNum, false);
        currentPageNum = next;
        bufMgr->readPage(file, currentPageNum, currentPageData);
        leaf = (LeafNodeInt*)currentPageData;
        nextEntry = 0;
    }
    if (leaf->keyArray[nextEntry] >= highValInt) {
        bufMgr->unPinPage(file, currentPageNum, false);
        throw IndexScanCompletedException();
    }
    outRid = leaf->ridArray[nextEntry];
    nextEntry += 1;
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
/**
   * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
   * @throws ScanNotInitializedException If no scan has been initialized.
  **/
const void BTreeIndex::endScan() 
{
    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }
    scanExecuting = false;
}

}
