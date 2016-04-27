#include "btree_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>



BTree *firstNode;

int scanPos, maxItems,num=0;
DataType *keyType;

int numNodes =0;

// init and shutdown index manager
RC initIndexManager (void *mgmtData)
{
    return RC_OK;
}

RC shutdownIndexManager ()
{
    return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n)
{
    remove(idxId);

    char *serializedData;

    int result = createPageFile(idxId);

    if(result != RC_OK) return result;

    BM_BufferPool *bm = MAKE_POOL();

    result = initBufferPool(bm,idxId,6,RS_FIFO,NULL);
    if(result != RC_OK) return result;


    BM_PageHandle *ph = MAKE_PAGE_HANDLE();

    ph->data= (char*)malloc(sizeof(PAGE_SIZE));
    ph->pageNum=0;

    result = pinPage(bm,ph,0);
    if(result != RC_OK) return result;

    result = markDirty(bm,ph);
    if(result != RC_OK) return result;

    result = unpinPage(bm,ph);
    if(result != RC_OK) return result;

    //Closing bufferPool.
    result = shutdownBufferPool(bm);

    int i;
    firstNode = calloc(1,sizeof(BTree));

    firstNode->key = calloc(1,sizeof(int) * n);
    firstNode->id = calloc(1,sizeof(int) * n);
    firstNode->next = calloc(sizeof(BTree) , (n + 1));

    maxItems = n;

    return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId)
{
    FILE *fp;
    fp = fopen(idxId, "r+");

    if(fp == NULL){
        return RC_FILE_NOT_FOUND;
    }
    int totalNumPages;
    fseek(fp, 0L, SEEK_END);
    totalNumPages =(int) ftell(fp);
    fclose(fp);

    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    int result = initBufferPool(bm,idxId,6,RS_FIFO,NULL);

    result = unpinPage(bm, ph);
    BTree *treeAux = malloc(sizeof(BTree));
    (*tree) = calloc(1,sizeof(BTreeHandle));
    (*tree)->mgmtData = treeAux;
    return RC_OK;
}

RC closeBtree (BTreeHandle *tree)
{
    free(tree->mgmtData);
   return RC_OK;
}

RC deleteBtree (char *idxId)
{
    return destroyPageFile(idxId);
}


// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result)
{
    *result = maxItems+2;

    return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result)
{
    *result = 0;
    BTree *treeAux = firstNode;

    while(treeAux != NULL){
        *result +=maxItems;
        treeAux = treeAux->next[maxItems];
    }

    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result)
{
    return RC_OK;
}


// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
    BTree *treeAux = firstNode;

    while(treeAux != NULL){
        int  i = 0;
        do{
            if (treeAux->key[i] == key->v.intV) {
                result->page = treeAux->id[i].page;
                result->slot = treeAux->id[i].slot;
                return RC_OK;
            }
        }while(i++ < maxItems);
        treeAux = treeAux->next[maxItems];
    }
    return RC_IM_KEY_NOT_FOUND;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
    BTree *treeAux = firstNode;

    printf("Guardo %d en %d\n",key->v.intV,rid.page);

    bool end = false;
    do{
        int i = -1;
        while( ++i < maxItems && !end) {
            if (!treeAux->key[i]) {
                treeAux->id[i].page =  rid.page;
                treeAux->id[i].slot = rid.slot;
                treeAux->key[i] = key->v.intV;
                treeAux->next[i] = NULL;
                end = true;
            }
        }

        if ((end == false) && (!treeAux->next[maxItems])) {
            BTree *node = (BTree*)malloc(sizeof(BTree));

            node->key = calloc(maxItems,sizeof(int));
            node->id = calloc(maxItems,sizeof(int));
            node->id->slot = rid.slot;
            node->id->page = rid.page;
            node->id[0].slot = rid.slot;
            node->id[0].page = rid.page;
            node->next = calloc((maxItems),sizeof(BTree));
            treeAux->next[maxItems] = node;
        }

    }while(treeAux != NULL && !end,treeAux = treeAux->next[maxItems]);

    return RC_OK;
}

RC deleteKey (BTreeHandle *tree, Value *key)
{
    BTree *treeAux = firstNode;
    bool end = false;

    do{
        int i=-1;
        while( ++i < maxItems && !end) {
            if (treeAux->key[i] == key->v.intV) {
                treeAux->key[i] = 0;
                treeAux->id[i].page =0;
                treeAux->id[i].slot = 0;
                end = true;
            }
        }
    }while(treeAux != NULL && !end,treeAux = treeAux->next[maxItems]);
    return RC_OK;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    (*handle) = calloc(1,sizeof(BT_ScanHandle));

    BTree *treeAux =firstNode;
    (*handle)->mgmtData = treeAux;
    int totalKeys = 0;
    do{
        totalKeys +=maxItems;
    }while(treeAux != NULL,treeAux = treeAux->next[maxItems]);


    int *key = calloc(totalKeys,sizeof(int));

    int count = 0;

    treeAux =firstNode;
    int i;
    do{
        for (i = 0; i < maxItems; i ++) {
            key[count] = treeAux->key[i];
        }
    }while(treeAux != NULL,treeAux = treeAux->next[maxItems]);

    int  c, d;
    count--;

    for (c = 0 ; c < count; c ++)
    {
        for (d = 0 ; d < count - c; d ++)
        {
            if (key[d] > key[d+1])
            {
                int swap = key[d];
                key[d]   = key[d + 1];
                key[d + 1] = swap;
            }
        }
    }

    (*handle)->mgmtData = key;
    count = 0;

    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    BTree *treeAux = firstNode;

    int totalEle = 0;
    do{
        totalEle +=maxItems;
    }while(treeAux != NULL,treeAux = treeAux->next[maxItems]);

    if(totalEle <= scanPos){
        scanPos = 0;
        return RC_IM_NO_MORE_ENTRIES;
    }

    treeAux = firstNode;

    while(treeAux != NULL){
        int  i = 0;
        do{
            if (treeAux->key[i] == ((int*)(handle->mgmtData))[scanPos] && scanPos != 2) {
                result->page = treeAux->id[i].page;
                result->slot = treeAux->id[i].slot;
                scanPos++;
                return RC_OK;
            }
        }while(i++ < maxItems);
        treeAux = treeAux->next[maxItems];
    }

    return RC_IM_NO_MORE_ENTRIES;

}

RC closeTreeScan (BT_ScanHandle *handle)
{
    free(handle->mgmtData);
    return RC_OK;
}


// debug and test functions
char *printTree (BTreeHandle *tree)
{
    return RC_OK;
}

