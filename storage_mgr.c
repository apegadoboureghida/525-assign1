//
// Created by Andres on 31/01/16.
//

#include <unistd.h>
#include "storage_mgr.h"


/* manipulating page files */
void initStorageManager(void) {

}

/*
 * Function Name: createPageFile
 *
 * Description:
 *      Create a new page file fileName.
 *      The initial file size should be one page.
 *      This method should fill this single page with '\0' bytes
 *
 * Parameters:
 *      char *fileName: File name
 *
 * Return:
 *      RC: Returned Code
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
RC createPageFile(char *fileName) {
    FILE *fp;

    //r+ opening in the beginning
    fp=fopen(fileName, "w");

    //On Error return 0

    if(fp == NULL){
        return RC_WRITE_FAILED;
    }


    fclose(fp);

    //filling file with \0 bytes
    if(truncate(fileName,PAGE_SIZE) != 0)
    {
        return RC_WRITE_FAILED;
    }

    return RC_OK;
}

/*
 * Function Name: openPageFile
 *
 * Description:
 *      Opens an existing page file.
 *      Should return RC_FILE_NOT_FOUND if the file does not exist.
 *      If opening the file is successful,
 *      then the fields of this file handle should be initialized with the information about the opened file.
 *      For instance, you would have to read the total number of pages that are stored in the file from disk.
 *
 * Parameters:
 *      char *fileName: File name
 *      SM_FileHandle *fHandle: Existing file handle
 *
 * Return:
 *      RC: Returned Code
 *          Should return RC_FILE_NOT_FOUND if the file does not exist.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    //r+ opening in the beginning
    FILE *fp=fopen(fileName, "r+");

    if(fp == NULL){
        return RC_FILE_NOT_FOUND;
    }

    //totalNumPages
    int totalNumPAges;
    fseek(fp, 0L, SEEK_END);
    totalNumPAges = ftell(fp);
    totalNumPAges = totalNumPAges/PAGE_SIZE;

    fscanf (fp, "%d", &totalNumPAges);

    //Initializing fHandle with page data.
    fHandle->totalNumPages=totalNumPAges;
    fHandle->curPagePos=0;
    fHandle->fileName=fileName;
    
    fclose(fp);

    return RC_OK;
}

/*
 * Function Name: closePageFile
 *
 * Description:
 *      Close an open page file
 *
 * Parameters:
 *      SM_FileHandle *fHandle: Existing file handle
 *
 * Return:
 *      RC: Returned Code
 *          Should return RC_FILE_NOT_FOUND if the file does not exist.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
RC closePageFile(SM_FileHandle *fHandle) {

    FILE *fp=fopen(fHandle->fileName, "r+");

    if(fp == NULL){
        return RC_FILE_NOT_FOUND;
    }

    fHandle->totalNumPages=0;
    fHandle->fileName="";
    fHandle->curPagePos=0;
    
    fclose(fp);

    return RC_OK;
}

/*
 * Function Name: destroyPageFile
 *
 * Description:
 *      Close an open page file or destroy (delete) a page file
 *
 * Parameters:
 *      char *fileName: File name
 *
 * Return:
 *      RC: Returned Code
 *          Should return RC_FILE_NOT_FOUND if the file does not exist.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
RC destroyPageFile(char *fileName) {

    FILE *fp=fopen(fHandle->fileName, "r");
    
    if(fp==NULL)
    {
        fclose(fp);
        return RC_FILE_NOT_FOUND;
    }

    if(remove(fileName) != 0){
        return RC_REMOVE_FAILED;
    }
    return RC_OK;
}

/*
 * Function Name: readBlock
 *
 * Description:
 *      The method reads the pageNum block from a file and stores its content
 *      in the memory pointed to by the memPage page handle.
 *
 * Parameters:
 *      int pageNum: Page number
 *      SM_fileHandle *fHandle: Existing file handle
 *      SM_PageHandle memPage: Existing page handle
 *
 * Return:
 *      RC: Returned Code
 *          Should return RC_READ_NON_EXISTING_PAG, if the file has less than pageNum pages.
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if(pageNum <0){
        return RC_READ_NON_EXISTING_PAGE;
    }
        //Check totalNumPages
    if(fHandle->totalNumPages<=pageNum){
        return RC_READ_NON_EXISTING_PAGE;
    }
    FILE *fp;

    fp = fopen(fHandle->fileName,"r");
    
    fseek(fp, PAGE_SIZE*(pageNum), SEEK_SET);

    if(0 == fread(memPage, sizeof(char), PAGE_SIZE, fp)){
        fclose(fp);
        return  RC_READ_NON_EXISTING_PAGE;
    }
    fclose(fp);

    fHandle->curPagePos=pageNum;

    return RC_OK;
}

/*
 * Function Name: getBlockPos
 *
 * Description:
 *      Return the current page position in a file
 *
 * Parameters:
 *      SM_fileHandle *fHandle: Existing file handle
 *
 * Return:
 *      RC: Returned Code
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
int getBlockPos(SM_FileHandle *fHandle) {

    return fHandle->curPagePos;
}

/*
 * Function Name: readFirstBlock
 *
 * Description:
 *      Read the first block in a file
 *
 * Parameters:
 *      SM_fileHandle *fHandle: Existing file handle
 *      SM_PageHandle memPage: Existing page handle
 *
 * Return:
 *      RC: Returned Code
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

    return readBlock(0,fHandle,memPage);
}

/*
 * Function Name: readPreviousBlock
 *
 * Description:
 *      Read the previous relative to the curPagePos of the file.
 *      The curPagePos should be moved to the page that was read.
 *      If the user tries to read a block before the first page of after the last page of the file,
 *      the method should return RC_READ_NON_EXISTING_PAGE.
 *
 * Parameters:
 *      SM_fileHandle *fHandle: Existing file handle
 *      SM_PageHandle memPage: Existing page handle
 *
 * Return:
 *      RC: Returned Code
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

    return readBlock(fHandle->curPagePos-1,fHandle,memPage);
}

/*
 * Function Name: readCurrentBlock
 *
 * Description:
 *      Read the current relative to the curPagePos of the file.
 *      The curPagePos should be moved to the page that was read.
 *      If the user tries to read a block before the first page of after the last page of the file,
 *      the method should return RC_READ_NON_EXISTING_PAGE.
 *
 * Parameters:
 *      SM_fileHandle *fHandle: Existing file handle
 *      SM_PageHandle memPage: Existing page handle
 *
 * Return:
 *      RC: Returned Code
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

    return readBlock(fHandle->curPagePos,fHandle,memPage);
}

/*
 * Function Name: readNextBlock
 *
 * Description:
 *      Read the next relative to the curPagePos of the file.
 *      The curPagePos should be moved to the page that was read.
 *      If the user tries to read a block before the first page of after the last page of the file,
 *      the method should return RC_READ_NON_EXISTING_PAGE.
 *
 * Parameters:
 *      SM_fileHandle *fHandle: Existing file handle
 *      SM_PageHandle memPage: Existing page handle
 *
 * Return:
 *      RC: Returned Code
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos+1,fHandle,memPage);
}

/*
 * Function Name: readNextBlock
 *
 * Description:
 *      Read the last relative to the curPagePos of the file.
 *      The curPagePos should be moved to the page that was read.
 *      If the user tries to read a block before the first page of after the last page of the file,
 *      the method should return RC_READ_NON_EXISTING_PAGE.
 *
 * Parameters:
 *      SM_fileHandle *fHandle: Existing file handle
 *      SM_PageHandle memPage: Existing page handle
 *
 * Return:
 *      RC: Returned Code
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages-1,fHandle,memPage);
}

/*
 * Function Name: writeBlock
 *
 * Description:
 *      Write a page to disk using either the current position or an absolute position.
 *
 * Parameters:
 *      int pageNum: Page number
 *      SM_fileHandle *fHandle: Existing file handle
 *      SM_PageHandle memPage: Existing page handle
 *
 * Return:
 *      RC: Returned Code
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      01/31/2016  Andres Pegado   Initialization
 */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if(pageNum <0){
        return RC_WRITE_FAILED;
    }
    FILE *fp;

    
    //Check totalNumPages
    if(fHandle->totalNumPages<pageNum){
        return RC_WRITE_FAILED;
    }

    fp = fopen(fHandle->fileName,"w");

    fseek(fp, PAGE_SIZE*(pageNum), SEEK_SET);
    if(fwrite(memPage,sizeof(char),sizeof(memPage),fp) == 0){
        fclose(fp);
        return  RC_WRITE_FAILED;
    }

    fclose(fp);

    fHandle->curPagePos=pageNum;

    return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(fHandle->curPagePos,fHandle,memPage);
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {

    fHandle->totalNumPages = fHandle->totalNumPages+1;

    //filling file with \0 bytes
    if(truncate(fHandle->fileName,(PAGE_SIZE*fHandle->totalNumPages)) != 0)
    {
        fHandle->totalNumPages = fHandle->totalNumPages-1;
        return RC_WRITE_FAILED;
    }


    return RC_OK;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {

    if(numberOfPages > fHandle->totalNumPages){
        fHandle->totalNumPages = numberOfPages;
        //filling file with \0 bytes
        if(truncate(fHandle->fileName,(PAGE_SIZE*fHandle->totalNumPages)) != 0)
        {
            return RC_WRITE_FAILED;
        }
    }

    return RC_OK;
}
