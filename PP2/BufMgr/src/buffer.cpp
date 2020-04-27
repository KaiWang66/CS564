/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

    static int bufsize;
    BufMgr::BufMgr(std::uint32_t bufs)
            : numBufs(bufs) {
        bufDescTable = new BufDesc[bufs];
        bufsize = (int)(bufs);
        for (FrameId i = 0; i < bufs; i++)
        {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
        }

        bufPool = new Page[bufs];
        int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
        hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

        clockHand = bufs - 1;
    }


    BufMgr::~BufMgr() {
        // flush out all dirty pages
        for (int i = 0; i < bufsize; i++)
        {
            if (bufDescTable[i].dirty) {
                bufDescTable[i].file->writePage(bufPool[i]);
            }
        }
        delete [] bufPool;
        delete [] bufDescTable;
    }

    void BufMgr::advanceClock()
    {
        clockHand = (clockHand + 1) % bufsize;
    }

    void BufMgr::allocBuf(FrameId & frame)
    {
        bool found = false;
        // check if all frames are pinned
        int numOfPinned = 0;
        for (int i = 0; i < bufsize; i++) {
            if (bufDescTable[i].pinCnt != 0) {
                numOfPinned++;
            }
        }
        // throw exceptions if all frames are pinned
        if (numOfPinned == bufsize) {
            throw BufferExceededException();
        }
        while(!found) {
            advanceClock();
            // if refbit is true, reset it and skip
            if (bufDescTable[clockHand].refbit) {
                bufDescTable[clockHand].refbit = false;
                continue;
            }
            // if pinCnt != 0, skip
            if (bufDescTable[clockHand].pinCnt != 0) {
                continue;
            }
            // if dirty, flush it
            if (bufDescTable[clockHand].dirty) {
                bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                bufDescTable[clockHand].dirty = false;
            }
            // if contains valid page, delete it from the hashtable
            if (bufDescTable[clockHand].valid) {
                hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
            }
            frame = clockHand;
            found = true;
        }
    }


    void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
    {
        FrameId frameNo;
        // look up the page in the hashtable
        try {
            hashTable->lookup(file, pageNo, frameNo);
            // set refbit
            bufDescTable[frameNo].refbit = true;
            bufDescTable[frameNo].pinCnt = bufDescTable[frameNo].pinCnt + 1;
            page = &(bufPool[frameNo]); // return through ref
        } catch (HashNotFoundException e) {
            // if the page isn't in the pool, allocate a new frame
            allocBuf(frameNo);
            Page pageFromDisk = file->readPage(pageNo);
            hashTable->insert(file, pageNo, frameNo);
            bufDescTable[frameNo].Set(file, pageNo);
            bufPool[frameNo] = pageFromDisk;
            page = &(bufPool[frameNo]);
        }
    }


    void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
    {
        FrameId frameNo;
        // look up the page in hashTable
        try {
            hashTable->lookup(file, pageNo, frameNo);
//        std::cout << "unpinPage:" << frameNo << "\n";
            if (bufDescTable[frameNo].pinCnt == 0) {
                throw PageNotPinnedException(file->filename(), pageNo, frameNo);
            }
            // decrement the pinCnt
            bufDescTable[frameNo].pinCnt -= 1;
            // set the dirty bit if dirty
            if (dirty) {
                bufDescTable[frameNo].dirty = true;
            }
        } catch (HashNotFoundException e) {
            // do nothing
        }
    }

    void BufMgr::flushFile(const File* file)
    {
        for (int i = 0; i < bufsize; i++) {
            if (bufDescTable[i].valid && bufDescTable[i].file == file) {
                if (bufDescTable[i].pinCnt != 0) {
                    throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
                }
                if (bufDescTable[i].dirty) {
                    bufDescTable[i].file->writePage(bufPool[i]);
                    bufDescTable[i].dirty = false;
                }
                hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
                bufDescTable[i].Clear();
            }
            if (!bufDescTable[i].valid && bufDescTable[i].file == file) {
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty,
                                         bufDescTable[i].valid, bufDescTable[i].refbit);
            }
        }
    }

    void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
    {
        Page newPage = file->allocatePage();
        FrameId frameNo;
        allocBuf(frameNo);
        // set up hashtable and descTable
        hashTable->insert(file, newPage.page_number(), frameNo);
        bufDescTable[frameNo].Set(file, newPage.page_number());
        bufPool[frameNo] = newPage;
        pageNo = newPage.page_number();
        page = &(bufPool[frameNo]);
    }

    void BufMgr::disposePage(File* file, const PageId PageNo)
    {
        FrameId frameNo = -1;
        // look up the page in the hashtable
        try {
            hashTable->lookup(file, PageNo, frameNo);
            if (bufDescTable[frameNo].dirty) {
                bufDescTable[frameNo].file->writePage(bufPool[frameNo]);
            }
            hashTable->remove(bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);
            bufDescTable[frameNo].Clear();
        } catch (HashNotFoundException e) {
            // do nothing
        }
        file->deletePage(PageNo);
    }

    void BufMgr::printSelf(void)
    {
        BufDesc* tmpbuf;
        int validFrames = 0;

        for (std::uint32_t i = 0; i < numBufs; i++)
        {
            tmpbuf = &(bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpbuf->Print();

            if (tmpbuf->valid)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

}
