//
// Created by apegadoboureghida on 21/03/16.
//

#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"


// table and manager
int initRecordManager(void *mgmtData) {
    return RC_OK;
}

int shutdownRecordManager() {
    return RC_OK;
}

int createTable(char *name,Schema *schema) {
    RM_TableData *tData = (RM_TableData*) malloc (sizeof(RM_TableData));

    schema=(Schema *) malloc(sizeof(Schema));
    schema->numAttr = 0;
    //creating page file
    initStorageManager();
    int result = createPageFile(name);

    if(result != RC_OK) return result;

    //Init buffer pool.
    BM_BufferPool *bm = MAKE_POOL();

    result = initBufferPool(bm,name,3,RS_FIFO,NULL);

    if(result != RC_OK) return result;

    //init Table info
    tData->name = name;
    tData->schema = schema;

    tData->mgmtData = bm;

    //Page 0 store table information
    char *serilizedTData= serializeTableContent(tData);

    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    ph->data= (char*)malloc(sizeof(PAGE_SIZE));
    ph->pageNum=0;

    pinPage(bm,ph,0); //Todo check RC_OK
    memcpy(ph->data,serilizedTData,PAGE_SIZE);
printf("data: %s\n",serilizedTData);
    markDirty(bm,ph); //Todo check RC_OK
    unpinPage(bm,ph); //Todo check RC_OK
    //Closing bufferPool.
    shutdownBufferPool(bm);
    return RC_OK;
}

int openTable(RM_TableData *rel, char *name) {
    printf("openTable\n");

    BM_BufferPool *bm = MAKE_POOL();

    int result = initBufferPool(bm,name,3,RS_FIFO,NULL);
    if(result != RC_OK) return result;

    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    ph->data= (char*)malloc(sizeof(PAGE_SIZE));
    ph->pageNum=0;

    pinPage(bm,ph,0); //Todo check RC_OK

    //Todo read schema
    rel->schema = (Schema*) malloc(sizeof(Schema));

    unpinPage(bm,ph);
    return RC_OK;
}

int closeTable(RM_TableData *rel) {
    shutdownBufferPool(rel->mgmtData);
    freeSchema(rel->schema);

    rel = (RM_TableData *) malloc(sizeof(RM_TableData));
    return RC_OK;
}

int deleteTable(char *name) {

    return destroyPageFile(name);
}

int getNumTuples(RM_TableData *rel) {
    //Todo
    return 0;
}

// handling records in a table

int insertRecord(RM_TableData *rel, Record *record) {
    return 0;
}

int deleteRecord(RM_TableData *rel,RID id) {
    return 0;
}

int updateRecord(RM_TableData *rel, Record *record) {
    return 0;
}

int getRecord(RM_TableData *rel,RID id,Record *record) {

    BM_PageHandle *bm = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
    int result = pinPage(rel->mgmtData,bm,id.page);

    if(result != RC_OK) return result;

    char *pointer = bm->data+(getRecordSize(rel->schema)*id.slot);

    memcpy(record->data,pointer,getRecordSize(rel->schema));
    record->id=id;

    result = unpinPage(rel->mgmtData,bm);

    if(result != RC_OK) return result;

    return RC_OK;
}

// scans
int startScan(RM_TableData *rel,RM_ScanHandle *scan,Expr *cond) {

    scan->rel = rel;

    RID *rid = (RID *)malloc(sizeof(RID));
    rid->page=1;
    rid->slot=0;
    scan->mgmtData = rid;
    //TODO add cond.

    return RC_OK;
}

int next(RM_ScanHandle *scan,Record *record) {
    //TODO use cond

    record = (Record *)  malloc(sizeof(record));
    RID *rid = scan->mgmtData;
    getRecord(scan->rel,*rid,record);

    rid->slot++;
    BM_BufferPool *bm =scan->mgmtData;
    if(PAGE_SIZE<=rid->slot*getRecordSize(scan->rel->schema)){
        rid->slot=0;
        rid->page++;
    }else
    if(bm->numPages< rid->page){
        return RC_RM_NO_MORE_TUPLES;
    }
    //Check number of slots.
    return RC_OK;
}

int closeScan(RM_ScanHandle *scan) {
    return 0;
}

// dealing with schemas

int getRecordSize(Schema *schema) {

    int totalSize =0;
    int i=0;

    for(i;i<schema->numAttr;i++){
        totalSize+=schema->typeLength[i];
    }
    return totalSize;
}

struct Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength,
                            int keySize, int *keys) {

    Schema *sc = (Schema*) malloc(sizeof(Schema));

    sc->numAttr = numAttr;
    sc->attrNames = attrNames;
    sc->dataTypes = dataTypes;
    sc->typeLength = typeLength;
    sc->keySize = keySize;
    sc->keyAttrs = keys;

    return sc;
}

int freeSchema(Schema *schema) {
    return 0;
}

// dealing with records and attribute values
int createRecord(Record **record, Schema *schema) {
    return 0;
}

int freeRecord(Record *record) {
    return 0;
}

int getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    return 0;
}

int setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    return 0;
}
