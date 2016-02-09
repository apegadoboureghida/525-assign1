#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);

/* main function running all tests */
int
main (void)
{
    testName = "";

    initStorageManager();

    testSinglePageContent();

    return 0;
}



/* Try to create, open, and close a page file */
void
testSinglePageContent(void)
{
    SM_FileHandle fh;
    SM_PageHandle ph;
    int i;

    testName = "test single page content";

    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    // create a new page file
    TEST_CHECK(createPageFile (TESTPF));
    TEST_CHECK(openPageFile (TESTPF, &fh));
    printf("created and opened file\n");
    // read next page (Non-exist)

    ASSERT_TRUE(readNextBlock (&fh, ph)==RC_READ_NON_EXISTING_PAGE,"Try to read non existing page");

    // append Empty page
    TEST_CHECK(appendEmptyBlock(&fh));

    // read next page
    TEST_CHECK(readNextBlock (&fh, ph))
    // the page should be empty (zero bytes)
    for (i=0; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
    printf("next block was empty\n");

    // change ph to be a string and write that one to disk
    printf("currentBlock %d\n",fh.curPagePos);
    for (i=0; i < PAGE_SIZE; i++)
        ph[i] = (i % 10) + '0';
    TEST_CHECK(writeCurrentBlock(&fh,ph));
    printf("writing first block \n");

    TEST_CHECK(readCurrentBlock (&fh, ph));
    printf("currentBlock %d\n",fh.curPagePos);
    for (i=0; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
    printf("reading current block\n");

    //read previous
    TEST_CHECK(readPreviousBlock (&fh, ph));

    //read previous (non-existing)
    ASSERT_TRUE(readPreviousBlock (&fh, ph)== RC_READ_NON_EXISTING_PAGE,"Try to read non existing page");
    // read back the page containing the string and check that it is correct

    TEST_CHECK(readLastBlock (&fh, ph));
    printf("readLastBlock %d\n",fh.curPagePos);
    for (i=0; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
    printf("reading last block\n");

    //get position
    TEST_CHECK(getBlockPos(&fh)==2);

    //EnsureCapacity
    TEST_CHECK(ensureCapacity(3,&fh));

    //go to last
    TEST_CHECK(readLastBlock(&fh,ph));

    //get position
    TEST_CHECK(getBlockPos(&fh)==3)

    // destroy new page file
    TEST_CHECK(destroyPageFile (TESTPF));

    TEST_DONE();
}
