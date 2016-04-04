

#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <string.h>

typedef struct MgmtData {
    SM_FileHandle fHandle;

    int *fixCount ; //Array of size numPages Fix counter
    bool *dirtyPin; // Dirty page
    int readIO; // ReadIO counter
    int writeIO; // WriteIO counter
    int *fifoStat; // fifo stats
    int *lruStat; // lru stats
    int readBuff; // Read from buffer counter
    BM_PageHandle **buffer; // Buffer data
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
 *      02/24/2016  Andres Pegado   Updates
 */

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages,
                  ReplacementStrategy strategy, void *stratData) {

    SM_FileHandle *fHandle = (SM_FileHandle*) malloc (sizeof(SM_FileHandle));


    int result =openPageFile((char *)pageFileName,fHandle);


    if(result!=0)
        return result;

    char *fileName = (char *) malloc((strlen(pageFileName)+1));
    strcpy(fileName, pageFileName);


    bm->numPages=numPages;
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
    int y =0;
    for(y; y< bm->numPages;y++){
        if(data->fixCount[y]>0){
            return RC_WRITE_FAILED;
        }
    }

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

    SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(fHandle));
    int result = openPageFile(bm->pageFile,fHandle);
    if(result!=0)
        return result;
    int i=0;
    for(i;i<bm->numPages;i++)
    {

        if(data->fixCount[i]==0 && data->dirtyPin[i]== true)
        {
            result = writeBlock(data->buffer[i]->pageNum,fHandle,data->buffer[i]->data);
            if(result!=0)
                return result;
            data->dirtyPin[i]=false;
            data->writeIO++;
        }
    }

    free(fHandle);

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
        if(temp->pageNum== page->pageNum){
            position= y;
            data->fixCount[position]--;
            break;
        }
    }
    if(position>=0){

        return RC_OK;
    }else{
        return RC_READ_NON_EXISTING_PAGE;
    }


}
/*
 * Function Name: forcePage
 *
 * Description:
 *      Writes the current content of the page back to disk.
 *      Checks for page existence in buffer.
 *      Write the data from buffer to disk.
 *       
 * Parameters:
 *      BM_BufferPool *const bm: Buffer pool object
 *	BM_PageHandle *const page: Buffer pool page handle 
 *
 * Return:
 *      RC: Returned Code
 *          Should return RC_OK.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/22/2016  Mansi Malviya   Initialization
 */
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {

    SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(fHandle));
    int result = openPageFile(bm->pageFile,fHandle);
    if(result!=0)
        return result;
    writeBlock(page->pageNum,fHandle,page->data);


    MgmtData *data = bm->mgmtData;
    data->writeIO++;

    free(fHandle);
    return RC_OK;
}
/*
 * Function Name: pinPage
 *
 * Description:
 *      Pins the page in buffer with the pageNum.
 *      Checks for page existence in buffer if it existes then mark the position.
 *      Uses freeFrame for strategy, allocates memory,read the data from pageNum and pin it in buffer.
 *       
 * Parameters:
 *      BM_BufferPool *const bm: Buffer pool object
 *	BM_PageHandle *const page: Buffer pool page handle 
 *	const PageNumber pageNum: Number of the page to be pinned.
 *
 * Return:
 *      RC: Returned Code
 *          Should return RC_OK.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/22/2016  Andres Pegado   Initialization
 *      02/26/2016  Bill Molchan    Updates
 */
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {

    printf("0.1\n");
    //Check if it's already on buffer;
    int y= 0;
    int position=-1;
    MgmtData *data = bm->mgmtData;
    printf("0.2\n");
    SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(fHandle));
    printf("0.3 %s\n",bm->pageFile);
    int result = openPageFile(bm->pageFile,fHandle);
    printf("0.4\n");
    if(result!=0)
        return result;

    printf("1\n");
    for(y;y<bm->numPages;y++)
    {
        BM_PageHandle *temp = data->buffer[y];
        if(temp != NULL && temp->pageNum== pageNum){
            position= y;
        }
    }
    printf("2\n");
    BM_PageHandle *page2 = (BM_PageHandle *) calloc(1,sizeof(BM_PageHandle));
    //If page not exist
    if(position == -1){
        position =freeFrame(pageNum,bm);

        page2->data = (char *) calloc(1,PAGE_SIZE);
        page2->pageNum=pageNum;
        if(fHandle->totalNumPages<pageNum) {
            ensureCapacity(pageNum+1,fHandle);
        }
        //Fifo Stats
        readBlock(pageNum,fHandle,page2->data);

        data->fifoStat[position]=data->readIO++;
        data->buffer[position] = page2;
    }
    printf("3\n");
    page->data = data->buffer[position]->data;
    page->pageNum = data->buffer[position]->pageNum;
    data->fixCount[position]++;

    data->lruStat[position] = ++data->readBuff;

    free(fHandle);
    return RC_OK;
}

/*
 * Function Name: freeFrame
 *
 * Description:
 *      Handles replacement startegy to be used for page replacement.
 *      Uses fifo and lru stat which are arrays defined per each buffer page.
 *      Checks for dirty page and updates the position .
 *       
 * Parameters:
 *      BM_BufferPool *const bm: Buffer pool object
 *	    int pageNum: Number of the page to be replaced.
 *
 * Return:
 *      RC: Returned position of where the page should be replaced.
 *          
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/22/2016  Andres Pegado   Initialization
 *      02/24/2016  Mansi Malviya   Updates
 *      02/26/2016  Bill Molchan    Updates
 *      02/26/2016  Pratishtha      Updates    
 */

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
        forcePage(bm,data->buffer[position]);
        data->dirtyPin[position] = false;
    };
    return position;

}
/*
 * Function Name: getFrameContents
 *
 * Description:
 *      Checks all the pages in the frame and get the content of the page.
 *       
 * Parameters:
 *      BM_BufferPool *const bm: Buffer pool object
 *
 * Return:
 *      RC: Returned Content of the page.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/28/2016  Bill Molchan   Initialization
 */
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
/*
 * Function Name: getDirtyFlags
 *
 * Description:
 *      Checks all the dirty flags so far occured in the program.
 *       
 * Parameters:
 *      BM_BufferPool *const bm: Buffer pool object
 *
 * Return:
 *      RC: Returned no. dirty flags.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/1/2016  Bill Molchan       Initialization
 */
bool *getDirtyFlags(BM_BufferPool *const bm) {

    MgmtData *data=bm->mgmtData;

    return data->dirtyPin;
}

/*
 * Function Name: getFixCounts
 *
 * Description:
 *      Checks all the fix counts so far occured in the program.
 *       
 * Parameters:
 *      BM_BufferPool *const bm: Buffer pool object
 *
 * Return:
 *      RC: Returned no. of fixcounts.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/1/2016   Pratishtha       Initialization
 */
int *getFixCounts(BM_BufferPool *const bm) {
    MgmtData *data=bm->mgmtData;


    return data->fixCount;
}
/*
 * Function Name: getNumReadIO
 *
 * Description:
 *      Checks all the pages read so far in the program.
 *       
 * Parameters:
 *      BM_BufferPool *const bm: Buffer pool object
 *
 * Return:
 *      RC: Returned no. of pages read.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/1/2016   Pratishtha       Initialization
 */
int getNumReadIO(BM_BufferPool *const bm) {

        MgmtData *data=bm->mgmtData;


        return data->readIO;
}
/*
 * Function Name: getNumReadIO
 *
 * Description:
 *      Checks all the pages written so far in the program.
 *       
 * Parameters:
 *      BM_BufferPool *const bm: Buffer pool object
 *
 * Return:
 *      RC: Returned no. of pages written.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/1/2016   Mansi Malviya   Initialization
 */
int getNumWriteIO(BM_BufferPool *const bm) {
    MgmtData *data=bm->mgmtData;


    return data->writeIO;
}
