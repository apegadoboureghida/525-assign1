//
// Created by apegadoboureghda on 02/20/16.
//

#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "intQueue.h"

typedef struct MgmtData {
    SM_FileHandle fHandle;

    int *fixCount;
    bool *dirtyPin;
    intQueue *lruStat;
    BM_PageHandle **buffer;
} MgmtData;

/*
 * Function Name: initBufferPool
 *
 * Description:
 *	Creates a buffer pool for an existing page file.
 *      Opens an existing page file.
 *      Initializes the buffer pool.
 *      Handles the memory allocation.
 *
 * Parameters:
 *      BM_BufferPool *const bm: Object for buffer pool
 *	const char *const pageFileName:Name of the page file associated with the buffer pool 
 *	const int numPages: Number of page frames
 *	ReplacementStrategy strategy:Page replacement strategy
 *	void *stratData:Pointer to bookkeeping data
 *
 * Return:
 *      RC: Returned Code
 *          Should return RC_OK.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Andres Pegado   Initialization
 */

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
    mgmtData->lruStat=(intQueue *) malloc(sizeof(intQueue));

    int y = 0;
    for(y; y< numPages;y++) {
        //mgmtData->frameRef[y]=-1;
        mgmtData->fixCount[y] = 0;
        mgmtData->dirtyPin[y] = false;
        mgmtData->buffer[y] = (BM_PageHandle *) malloc (sizeof(BM_PageHandle )*numPages);
        mgmtData->buffer[y]->pageNum = -1;
    }

    initQueue(mgmtData->lruStat,numPages);

    return RC_OK;
}

/*
 * Function Name: shutdownBufferPool
 *
 * Description:
 *      Destroys a buffer pool.
 *      Checks for user using he buffer and throws error RC_WRITE_FAILED.
 *      Uses forceFlushpool function to move dirty pages to disk.
 *       
 * Parameters:
 *      BM_BufferPool *const bm: BUffer pool object
 *
 * Return:
 *      RC: Returned Code
 *          Should return RC_OK.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Mansi Malviya   Initialization
 */

RC shutdownBufferPool(BM_BufferPool *const bm) {

    MgmtData *data = bm->mgmtData;
    //Check if someone is using the buffer.
    int y =0;
    for(y; y< bm->numPages;y++){
        if(data->fixCount[y]>0){
         return RC_WRITE_FAILED;
        }
    }
    //Move all dirty pages to hard disk.
    forceFlushPool(bm);

    return RC_OK;
}

/*
 * Function Name: forceFlushPool
 *
 * Description:
 *      Writes dirty pages from buffer pool to disk.
 *      Checks for fixCount to be zero and writes the page onto disk.
 *       
 * Parameters:
 *      BM_BufferPool *const bm: BUffer pool object
 *
 * Return:
 *      RC: Returned Code
 *          Should return RC_OK.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Mansi Malviya   Initialization
 */

RC forceFlushPool(BM_BufferPool *const bm) {

    MgmtData *data=bm->mgmtData;

    SM_FileHandle fHandle;
    int i=0;
    for(i;i<bm->numPages;i++)
    {
        if(data->fixCount[i]==0 && data->dirtyPin[i]== true)
        {
        writeBlock(data->buffer[i]->pageNum,&fHandle,data->buffer[i]->data);
        data->dirtyPin[i]==false;
        }
    }

    return RC_OK;
}

/*
 * Function Name: markDirty
 *
 * Description:
 *      It marks the page dirty.
 *      Checks for each page on buffer using page handle if not null then marks it dirty.
 *      Updates the dirtyPin array witht he position of page.
 *       
 * Parameters:
 *      BM_BufferPool *const bm: Buffer pool object
 *	BM_PageHandle *const page: Buffer pool page handle 
 *
 * Return:
 *      RC: Returned Code
 *          Should return RC_OK and no page exist in buffer then returns RC_READ_NON_EXISTING_PAGE.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/21/2016  Andres Pegado   Initialization
 */
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {

    MgmtData *data = bm->mgmtData;
    int y=0;
    int position = -1;
    for(y;y<bm->numPages;y++)
    {
        BM_PageHandle *temp = data->buffer[y];
        if(temp != NULL && temp->pageNum== page->pageNum){
            position= y;
        }
    }
    if(position >= 0){

        data->dirtyPin[position] = true;
        return RC_OK;
    }else{
        return RC_READ_NON_EXISTING_PAGE;
    }

}

/*
 * Function Name: unpinPage
 *
 * Description:
 *      Unpins a particular page from buffer.
 *      Takes pageNum as page to be unpinned from buffer.
 *      Updates the fixCount to decrease the position.
 *       
 * Parameters:
 *      BM_BufferPool *const bm: Buffer pool object
 *	BM_PageHandle *const page: Buffer pool page handle 
 *
 * Return:
 *      RC: Returned Code
 *          Should return RC_OK and no page exist in buffer then returns RC_READ_NON_EXISTING_PAGE.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/21/2016  Andres Pegado   Initialization
 */

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
        return RC_READ_NON_EXISTING_PAGE;
    }


}


RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    MgmtData *mgmtData = bm->mgmtData;
    SM_FileHandle fHandle;
    //Check if page exists in buffer
    int y=0;
    int position=-1;
    for(y;y<bm->numPages;y++)
    {
        BM_PageHandle *temp = data->buffer[y];
        if(temp != NULL && temp->pageNum== pageNum){
            position= y;
            writeBlock(data->buffer[y]->pageNum,&fHandle,data->buffer[y]->data);
            
        }
    }
    return RC_OK;
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
        position =freeFrame(pageNum,bm->strategy,*data,bm);
        page->data = (char *) malloc(PAGE_SIZE);
        //writeBlock(position,&fileHandle,page->data);
        //Fifo Stats
        BM_PageHandle *actualPage = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
        actualPage->data = (char*) malloc(sizeof(page->data));
        int result = readBlock(pageNum,&fileHandle,actualPage->data);
        actualPage->pageNum = pageNum;
        data->buffer[position] = actualPage;
        page->data = actualPage->data;
        page->pageNum=actualPage->pageNum;
        //data->frameRef[position]=pageNum;
    }else{
        page->data = data->buffer[position]->data;
    }

    page->pageNum=pageNum;

    data->fixCount[position]++;

    //Update LRU stats
    moveToBack(data->lruStat,position);

    return RC_OK;
}

//Todo
int freeFrame(int pageNum,ReplacementStrategy strategy,MgmtData data,BM_BufferPool *const bm){
    //Decide position
    //Todo: Now always return zero

    int position = -1;
    switch(strategy){
        case RS_LRU:

            while(position<0){
                
            }


            break;
    }
    int pos = 0;

    //Check if dirty
    if(data.dirtyPin[pos]){
        //Todo:if dirty back to memory
        forceFlushPool(bm);
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
