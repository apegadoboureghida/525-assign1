//
// Created by Andres on 31/01/16.
//

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
    return 0;
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
    return 0;
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
    return 0;
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
    return 0;
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
    return 0;
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
int getBlockPos(SM_FileHandle *fHandle) {
    return 0;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return 0;
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return 0;
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return 0;
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return 0;
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return 0;
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return 0;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return 0;
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {
    return 0;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    return 0;
}
