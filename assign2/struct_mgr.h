//
// Created by drow on 1/03/16.
//

#ifndef ASSIGN2_STRUCT_MGR_H
#define ASSIGN2_STRUCT_MGR_H

#endif //ASSIGN2_STRUCT_MGR_H

#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct replaceData{
    intQueue *FIFOq;
    intQueue *LRUq;
    intQueue *Clockq;
} replaceData;

typedef struct MgmtData {
    SM_FileHandle fHandle;
    replaceData *dataStat;
    int *fixCount;
    bool *dirtyPin;
    BM_PageHandle **buffer;
} MgmtData;