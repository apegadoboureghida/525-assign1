

Team Members:  Andres Pegado, Mansi Malviya, Pratishtha Verma
Date:    02/08/2016
Assignment 1

File list:
a)	storage_mgr.h: 

It is a header file which has entire interface that the storage manager should implement.
b)	storage_mgr.c: 
It is a file which consists of the code written by us in order to implement the said functions. 
c)	dberror.h:
      It is a header file which contains error codes, which will be used if any error arises.
d)	dberror.c: 
It helps to print a message describing the error
e)	test_helper.h: 
It is a header file which contains all the macros, which helps us know that whether the method implemented by us, works or not.
f)	test_assign_1_1.c: 
It contains a list of all the test cases given.  

Milestone: To implement a storage manager that performs the following tasks:
•	Manipulating page files 
•	Reading blocks from a file on disc into memory
•	Writing blocks to a page file on disk from memory
•	Maintaining information like:
o	Total number of pages
o	File name
o	Current page position
o	Management Information

Installation Instruction
After having all the files in respective directory, we have used the make command to compile both files. We have used 2 test files here, test_assign_1_1.c & test_assign_1_2.c. The first file contains a list of all the test cases given & the second file has a list of all the extra test cases created by us.  
After compiling the test files, both files can be run for respective checking using ./test_assign_1_1.c and ./test_assign_1_2.c. Program will run successfully for all the test cases with the dummy file used in program.

Function Description:


Manipulating Page Files

extern void initStorageManager (void);
This method is used to initialize the storage manager in order to carry out all respective functions.

extern RC createPageFile (char *fileName);
This method is used to create a file. A function called fopen() is used in order to create a file whose filename is given by the user in the “write” mode. If the file can’t be created, then an error is displayed. In the other case, using truncate function, a file will be created with a page, which is filled with 0’s. Basically an empty page will be created inside the newly created file.

extern RC openPageFile (char *fileName, SM_FileHandle *fHandle);
This function should return an error message, if the file does not exist. Here, the fseek() function is used. If the file is opened successfully, then the the fields of the file handle are initialized with the information about the opened file i.e. the page data information.



extern RC closePageFile (SM_FileHandle *fHandle);
This method is used to close the file. Here, if the file does not exist, fp will point to null. Otherwise, the totalNumPages & curPagePos is initialized to 0. 


extern RC destroyPageFile (char *fileName);
This method is used to delete/destroy a file. A function called remove() is used to delete the file requested by the user. The file will be deleted only if it exists otherwise it will return an error.












Reading blocks from a file on disc into memory

extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
This method is used to read a block. Each block has a page number which is calculated and as per the requested page number the block is found and written in the memory location pointed to by the memPage page handle. It will return an error, if the file has less than pageNum pages. 

extern int getBlockPos (SM_FileHandle *fHandle);
This method is used to return the current page position in a file specified fHandle pointer.

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
This method is used to read the first block in a file and it uses the pointer fHandle to move to the first block for reading it.


extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
This method is used to read the previous block which is relative to the curPagePos of file. If the user tries to read a block before the first page or after the last page of file, error is returned to user.




extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
This method is used to read the current block and it uses the fHandle pointer. The block is read relative to the curPagePos of the file. 



extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
This method is used to read the next block and it uses the fHandle pointer to move to the next block for reading it. The curPagePos should be moved to the page that was read. If the user tries to read a block before the first page of after the last page of the file, then the error is returned.

extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
This method is used to read the last block of file relative to curPagePos & it uses the fHandle pointer to move to the last block for reading it. The curPagePos should be moved to the page that was read. If the user tries to read a block before the first page of after the last page of the file, then the error is returned.




Additional error codes:
RC_REMOVED_FAILED: This error code is used in the function “destroyPageFile” in order to return an error when function is unable to remove the file.


Writing blocks to a page file on disk from memory

extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
This method is used to write a page to disk using either the current position or an absolute position of the file pointer. If the pageNum is less than 0, then an error message is displayed. Then fopen is used to open the file in “write” mode. Then fHandle is used to check that if the totalNumPages are less than pageNum. If yes, then again an error is displayed. If the curPagePos is equal to pageNum, then the “write” operation is finally performed.





extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
This method is used to write a block with reference to the curPagePos value. 


extern RC appendEmptyBlock (SM_FileHandle *fHandle);
This method is used to add or append an empty block at the end of the file in order to add more space and write data.


extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle);
This method is used to ensure the capacity of the file using truncate function. Using the total number of pages and comparing, the capacity of the file can be known. 

Data structure: 
Extra credit:
 Additional test cases
Additional files: 
test_assign_1_2.c: It contains a list of all the extra test cases. 
Test cases:
readNextBlock: 
Error is thrown when we try to read a block after last block.
appendEmptyBlock: 
Adding a blank page to the file.
writeCurrentBlock: 
Writing the current block to the file.
readCurrentBlock: 
Reading current block based on curPagePos. It should be expected page.
readPreviousBlock: 
Error is thrown when we try to read page before first page.
readLastBlock: 
reads the block written recently in the file.
getBlockPos: 
To get the current position of the block.
ensureCapacity: 
To given new number of pages in the file.
readLastBlock: 
To go to the last block of the file.
destroyPageFile: 
It would return respective error in case of destroying page in the file.
