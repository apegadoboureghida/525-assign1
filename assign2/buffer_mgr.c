//
// Created by apegadoboureghda on 26/02/16.
//

#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <string.h>

typedef struct MgmtData {
    SM_FileHandle fHandle;

    int *fixCount;
    bool *dirtyPin;
    int readIO;
    int writeIO;
    int *fifoStat;
    int *lruStat;
    int readBuff;
    BM_PageHandle **buffer;
} MgmtData;

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages,
                  ReplacementStrategy strategy, void *stratData) {

    SM_FileHandle *fHandle = (SM_FileHandle*) malloc (sizeof(SM_FileHandle));


    int result =openPageFile((char *)pageFileName,fHandle);


    if(result!=0)
        return result;

    //result = ensureCapacity(PAGES,fHandle);

    if(result!=0)
        return result;

    char *fileName ;
    fileName = malloc(sizeof(char) * strlen(pageFileName));
    strcpy(fileName, pageFileName);

    int numero =numPages;
    bm->numPages=numero;
    bm->pageFile= fileName;
    bm->strategy = strategy;
    bm->mgmtData=stratData;

    bm->mgmtData=(MgmtData *) malloc(sizeof(MgmtData));
    MgmtData *mgmtData = bm->mgmtData;

    mgmtData->fixCount=(int *) malloc (sizeof(int)*numPages);
    mgmtData->dirtyPin=(bool *) malloc (sizeof(bool)*numPages);
    mgmtData->buffer=(BM_PageHandle **) malloc (sizeof(BM_PageHandle *)*numPages);
    mgmtData->fifoStat=(int *) malloc (sizeof(int)*numPages);
    mgmtData->lruStat=(int *) malloc (sizeof(int)*numPages);
    mgmtData->readIO=0;
    int y = 0;
    for(y; y< numPages;y++) {
        mgmtData->fixCount[y] = 0;
        mgmtData->dirtyPin[y] = false;
        mgmtData->buffer[y] = (BM_PageHandle *) malloc (sizeof(BM_PageHandle )*numPages);
        mgmtData->buffer[y]->pageNum = NO_PAGE;
        mgmtData->fifoStat[y]= NO_PAGE;
        mgmtData->lruStat[y]= 0;
        mgmtData->readBuff=0;
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

    forceFlushPool(bm);

    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm) {

    MgmtData *data=bm->mgmtData;

    SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(fHandle));
    openPageFile(bm->pageFile,fHandle);
    int i=0;
    for(i;i<bm->numPages;i++)
    {

        if(data->fixCount[i]==0 && data->dirtyPin[i]== true)
        {
            writeBlock(data->buffer[i]->pageNum,fHandle,data->buffer[i]->data);
            data->dirtyPin[i]=false;
            data->writeIO++;
        }
    }

    return RC_OK;
}

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

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    //Check if it's already on buffer
    int y= 0;
    int position=-1;
    MgmtData *data = bm->mgmtData;
    for(y;y<bm->numPages;y++)
    {
        BM_PageHandle *temp = data->buffer[y];
        if(temp->pageNum== page->pageNum){
            position= y;
            data->fixCount[position]--;
            break;
        }
    }
    if(position>=0){

        return RC_OK;
    }else{
        return -1;
    }


}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {

    SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(fHandle));
    openPageFile(bm->pageFile,fHandle);

    writeBlock(page->pageNum,fHandle,page->data);


    return RC_OK;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {


    //Check if it's already on buffer;
    int y= 0;
    int position=-1;
    MgmtData *data = bm->mgmtData;
    SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(fHandle));
    openPageFile(bm->pageFile,fHandle);


    for(y;y<bm->numPages;y++)
    {
        BM_PageHandle *temp = data->buffer[y];
        if(temp != NULL && temp->pageNum== pageNum){
            position= y;
        }
    }
    BM_PageHandle *page2 = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
    //If page not exist
    if(position == -1){
        position =freeFrame(pageNum,bm);

        page2->data = (char *) malloc(PAGE_SIZE);
        page2->pageNum=pageNum;
        if(fHandle->totalNumPages<pageNum) {
            ensureCapacity(pageNum+1,fHandle);
        }
        //Fifo Stats
        int result = readBlock(pageNum,fHandle,page2->data);

        data->fifoStat[position]=data->readIO++;
        data->buffer[position] = page2;
    }else{
        //page = data->buffer[position];
    }

    page->data = data->buffer[position]->data;
    page->pageNum = data->buffer[position]->pageNum;
    data->fixCount[position]++;

    data->lruStat[position] = ++data->readBuff;
    return RC_OK;
}


//TODO
int freeFrame(int pageNum,BM_BufferPool *const bm){
    //Decide position
    //Todo: Now always return zero
    MgmtData *data = bm->mgmtData;
    int position = -1;
    int x =0;
    int lower =data->readIO;
    int least = clock();
    switch(bm->strategy){
        case RS_FIFO:
            for(x;x<bm->numPages;x++){
                if((data->fifoStat[x]<lower && data->fixCount[x]<=0)){
                    lower=data->fifoStat[x];
                    position=x;
                }
            }
            break;
        case RS_LRU:
            for(x;x<bm->numPages;x++){
                if((data->lruStat[x]<least && data->fixCount[x]<=0)){
                    least=data->lruStat[x];
                    position=x;
                }
            }
            break;
        default:
            return RC_RM_UNKOWN_DATATYPE;
    }

    if(position==-1){

        return RC_IM_NO_MORE_ENTRIES;
    }
    //Check if dirty
    if(data->dirtyPin[position]){
        //Todo:if dirty back to memory
        //forceFlushPool(bm);
        forcePage(bm,data->buffer[position]);
        data->dirtyPin[position] = false;
        data->writeIO++;
    };
    return position;

}

PageNumber *getFrameContents(BM_BufferPool *const bm) {

    PageNumber *contents =(int*) malloc(bm->numPages * sizeof(int));
    MgmtData *data=bm->mgmtData;


    int i=0;
    for(i;i<bm->numPages;i++)
    {
        contents[i]=data->buffer[i]->pageNum;
    }
    return contents;
}

bool *getDirtyFlags(BM_BufferPool *const bm) {

    MgmtData *data=bm->mgmtData;


    return data->dirtyPin;
}

int *getFixCounts(BM_BufferPool *const bm) {
    MgmtData *data=bm->mgmtData;


    return data->fixCount;
}

int getNumReadIO(BM_BufferPool *const bm) {

        MgmtData *data=bm->mgmtData;


        return data->readIO;
}

int getNumWriteIO(BM_BufferPool *const bm) {
    MgmtData *data=bm->mgmtData;


    return data->writeIO;
}
