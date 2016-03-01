//
// Created by apegadoboureghda on 26/02/16.
//

#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"


typedef struct MgmtData {
    SM_FileHandle fHandle;

    int *fixCount;
    bool *dirtyPin;
    BM_PageHandle **buffer;
} MgmtData;

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages,
                  ReplacementStrategy strategy, void *stratData) {

    SM_FileHandle fHandle;

    int result =openPageFile((char *)pageFileName,&fHandle);


    if(result!=0)
        return result;

    result = ensureCapacity(numPages,&fHandle);

    if(result!=0)
        return result;

    bm->numPages=numPages;
    bm->pageFile= pageFileName;
    bm->strategy = strategy;
    bm->mgmtData=stratData;

    bm->mgmtData=(MgmtData *) malloc(sizeof(MgmtData));
    MgmtData *mgmtData = bm->mgmtData;

    //mgmtData->frameRef=(int *) malloc (sizeof(int)*numPages);
    mgmtData->fixCount=(int *) malloc (sizeof(int)*numPages);
    mgmtData->dirtyPin=(bool *) malloc (sizeof(bool)*numPages);
    mgmtData->buffer=(BM_PageHandle **) malloc (sizeof(BM_PageHandle *)*numPages);

    int y = 0;
    for(y; y< numPages;y++) {
        //mgmtData->frameRef[y]=-1;
        mgmtData->fixCount[y] = 0;
        mgmtData->dirtyPin[y] = false;
        mgmtData->buffer[y] = (BM_PageHandle *) malloc (sizeof(BM_PageHandle )*numPages);
        mgmtData->buffer[y]->pageNum = -1;
    }

    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm) {

    MgmtData *data = bm->mgmtData;
    int y =0;
    for(y; y< bm->numPages;y++){
        if(data->fixCount[y]>0){
         return RC_WRITE_FAILED;
        }
    }
d
    forceFlushPool(bm);

    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm) {
    MgmtData *data=bm->mgmtData;
    int i=0;
    for(i;i<bm->numPages;i++)
    {
        if(data->fixCount[y]==0 && data->dirtyPin[y]== true)
        {
        writeBlock(numPages,&fHandle,page->data);
        data->dirtyPin[y]==False;
        }
    }

    return RC_OK;
}

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {

    return 0;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    //Check if it's already on buffer
    int y= 0;
    int position=-1;
    MgmtData *data = bm->mgmtData;
    for(y;y<bm->numPages;y++)
    {
        BM_PageHandle *temp = data->buffer[y];
        if(temp != NULL && temp->pageNum== page->pageNum){
            position= y;
        }
    }
    if(position>=0){
        data->fixCount[position]--;
        return RC_OK;
    }else{
        return -1;
    }


}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    return 0;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {


    //Check if it's already on buffer;
    int y= 0;
    int position=-1;
    MgmtData *data = bm->mgmtData;
    SM_FileHandle fileHandle = data->fHandle;


    for(y;y<bm->numPages;y++)
    {
        BM_PageHandle *temp = data->buffer[y];
        if(temp != NULL && temp->pageNum== pageNum){
            position= y;
        }
    }

    //If page not exist
    if(position == -1){
        position =freeFrame(pageNum,bm->strategy,*data);
        page->data = (char *) malloc(PAGE_SIZE);
        //writeBlock(position,&fileHandle,page->data);
        //Fifo Stats
        int result = readBlock(pageNum,&fileHandle,page->data);
        data->buffer[position] = page;
        //data->frameRef[position]=pageNum;
    }else{
        page->data = data->buffer[position]->data;
    }

    page->pageNum=pageNum;

    data->fixCount[position]++;
    /*TODO fix include and debug*/
    /*Then update the replacement strategies with the new information*/
   // updateLRU(target, bm); /*TODO update arguments*/
    //updateClock(target,bm); /*TODO update arguments*/
    return RC_OK;
}

//Todo
int freeFrame(int pageNum,ReplacementStrategy strategy,MgmtData data){
    //Decide position
    //Todo: Now always return zero
    int pos = 0;

    //Check if dirty
    if(data.dirtyPin[pos]){
        //Todo:if dirty back to memory

    }
    //return position
    return 0;
}

PageNumber *getFrameContents(BM_BufferPool *const bm) {
    return NULL;
}

bool *getDirtyFlags(BM_BufferPool *const bm) {
    return NULL;
}

int *getFixCounts(BM_BufferPool *const bm) {
    return NULL;
}

int getNumReadIO(BM_BufferPool *const bm) {
    return 0;
}

int getNumWriteIO(BM_BufferPool *const bm) {
    return 0;
}
