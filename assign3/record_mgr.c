#include "stdio.h"
#include "stdlib.h"
#include "string.h"

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"


typedef struct RM_ScanMgmt
{
    Expr *cond;
    RID *id;
}RM_ScanMgmt;

int totalPages;
bool existSoftDeleted;

extern RC initRecordManager (void *mgmtData)
{
    existSoftDeleted = true;
    return RC_OK;
}

extern RC shutdownRecordManager ()
{
    return RC_OK;
}



extern RC createTable (char *name, Schema *schema)
{
    remove(name);

    char *serializedData;

    int result = createPageFile(name);

    if(result != RC_OK) return result;

    BM_BufferPool *bm = MAKE_POOL();

    result = initBufferPool(bm,name,6,RS_FIFO,NULL);
    if(result != RC_OK) return result;

    serializedData = serializeSchema(schema);

    BM_PageHandle *ph = MAKE_PAGE_HANDLE();

    ph->data= (char*)malloc(sizeof(PAGE_SIZE));
    ph->pageNum=0;

    result = pinPage(bm,ph,0);
    if(result != RC_OK) return result;
    memcpy(ph->data,serializedData,PAGE_SIZE);

    result = markDirty(bm,ph);
    if(result != RC_OK) return result;

    result = unpinPage(bm,ph);
    if(result != RC_OK) return result;

    //Closing bufferPool.
    result = shutdownBufferPool(bm);

    return result;
}

extern RC openTable (RM_TableData *rel, char *name)
{
    FILE *fp;
    fp = fopen(name, "r+");

    if(fp == NULL){
        return RC_FILE_NOT_FOUND;
    }

    int totalNumPages;
    fseek(fp, 0L, SEEK_END);
    totalNumPages =(int) ftell(fp);
    totalPages = totalNumPages/PAGE_SIZE;

    fclose(fp);

    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();

    int result = initBufferPool(bm,name,6,RS_FIFO,NULL);
    if(result != RC_OK) return result;

    result = pinPage(bm, ph, 0);
    if(result != RC_OK) return result;

    rel->schema = deserializeSchema(ph->data);
    rel->name = name;
    rel->mgmtData = bm;

    result = unpinPage(bm, ph);

    return result;
}

extern RC closeTable (RM_TableData *rel)
{
    shutdownBufferPool((BM_BufferPool*)rel->mgmtData);
    free(rel->mgmtData);
    freeSchema(rel->schema);

    return RC_OK;
}

extern RC deleteTable (char *name)
{
    return destroyPageFile(name);
}

extern int getNumTuples (RM_TableData *rel)
{
    FILE *fp;
    fp = fopen(rel->name, "r+");
    int totalNumPages;
    fseek(fp, 0L, SEEK_END);
    totalNumPages =(int) ftell(fp);
    totalPages = totalNumPages/PAGE_SIZE;

    fclose(fp);

    return totalPages;// returning the count
}

extern RC insertRecord (RM_TableData *rel, Record *record)
{
    RID rid;
    rid.page = totalPages;
    rid.slot = 0;

    bool found = false;
    BM_PageHandle *page = MAKE_PAGE_HANDLE();

    if(existSoftDeleted){
        while(rid.page > 0 && rid.page < totalPages && !found)
        {
            pinPage((BM_BufferPool *)rel->mgmtData, page, rid.page++);

            if(strncmp(page->data,"DELETE",6) == 0){
                memset(page->data, '\0', PAGE_SIZE);
                strcpy(page->data,serializeRecord(record, rel->schema));

                markDirty((BM_BufferPool *)rel->mgmtData, page);
                forcePage((BM_BufferPool *)rel->mgmtData, page);

                unpinPage((BM_BufferPool *)rel->mgmtData, page);

                record->id.page=rid.page-1;
                record->id.slot=rid.slot;
                return RC_OK;
            }
            unpinPage(((BM_BufferPool *)rel->mgmtData), page);
        }
        record->id.page = rid.page;
    }else{
        record->id.page = totalPages;
    }

    if(rid.page == totalPages){
        existSoftDeleted = false;
    }

    record->id.slot = rid.slot;
    RC resultSig;
    resultSig = pinPage((BM_BufferPool *)rel->mgmtData, page,record->id.page);
    if(resultSig != RC_OK) return resultSig;
    memset(page->data, '\0', PAGE_SIZE);
    strcpy(page->data,serializeRecord(record, rel->schema));
    resultSig = markDirty((BM_BufferPool *)rel->mgmtData, page);
    if(resultSig != RC_OK) return resultSig;

    resultSig = forcePage((BM_BufferPool *)rel->mgmtData, page);
    if(resultSig != RC_OK) return resultSig;
    resultSig = unpinPage((BM_BufferPool *)rel->mgmtData, page);
    if(resultSig != RC_OK) return resultSig;

    totalPages++;

    return RC_OK;
}


extern RC deleteRecord (RM_TableData *rel, RID id)
{
    BM_PageHandle *page = MAKE_PAGE_HANDLE();

    pinPage((BM_BufferPool *)rel->mgmtData, page, id.page);

    char *temp = (char*)malloc(sizeof(PAGE_SIZE));
    strcpy(temp, "DELETED");

    page->pageNum = id.page;

    strcpy(page->data, temp);

    markDirty((BM_BufferPool *)rel->mgmtData, page);
    forcePage((BM_BufferPool *)rel->mgmtData, page);

    unpinPage((BM_BufferPool *)rel->mgmtData, page);

    return RC_OK;
}



extern RC updateRecord (RM_TableData *rel, Record *record)
{
    if(record->id.page > 0 && record->id.page <=  totalPages)
    {
        BM_PageHandle *page = MAKE_PAGE_HANDLE();

        pinPage((BM_BufferPool *)rel->mgmtData, page, record->id.page);

        memset(page->data, '\0', PAGE_SIZE);
        strcpy(page->data,serializeRecord(record,rel->schema));

        markDirty((BM_BufferPool *)rel->mgmtData, page);
        forcePage((BM_BufferPool *)rel->mgmtData, page);

        unpinPage((BM_BufferPool *)rel->mgmtData, page);

        return RC_OK;
    }
    else
    {
        return RC_RM_NO_MORE_TUPLES;
    }
}


extern RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    if(id.page > 0 && id.page <=  totalPages)
    {
        BM_PageHandle *page = MAKE_PAGE_HANDLE();

        pinPage((BM_BufferPool *)rel->mgmtData, page, id.page);

        char *record_data = (char*)malloc(PAGE_SIZE);
        strcpy(record_data,page->data);

        unpinPage((BM_BufferPool *)rel->mgmtData, page);

        if(!strncmp(page->data,"DELETED",7)){
            return RC_RM_NO_MORE_TUPLES;
        }
        Record *record_temp;
        deserializeRecord(record_data,rel->schema,&record_temp);

        *record = *record_temp;
        record->id.slot = id.slot;
        record->id.page = id.page;

        return RC_OK;
    }
    else
    {
        return RC_RM_NO_MORE_TUPLES;
    }
}

extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    RM_ScanMgmt *scan_mgmt = (RM_ScanMgmt*)malloc(sizeof(RM_ScanMgmt));

    scan_mgmt->cond = cond;

    scan_mgmt->id = (RID *) calloc(1,sizeof(RID));
    scan_mgmt->id->page = 1;
    scan_mgmt->id->slot = 0;

    scan->rel = rel;
    scan->mgmtData = scan_mgmt;

    return RC_OK;
}


extern RC next (RM_ScanHandle *scan, Record *record)
{
    Value *result = malloc(sizeof(Value));

    bool found = false;
    while((((RM_ScanMgmt *)scan->mgmtData)->id->page > 0 && ((RM_ScanMgmt *)scan->mgmtData)->id->page++ < totalPages) && !found)
    {
        getRecord (scan->rel, *((RM_ScanMgmt *)scan->mgmtData)->id, record);
        if(((RM_ScanMgmt *)scan->mgmtData)->cond) {
            evalExpr(record, scan->rel->schema, ((RM_ScanMgmt *) scan->mgmtData)->cond, &result);
        }
        if(!((RM_ScanMgmt *)scan->mgmtData)->cond || (result->dt == DT_BOOL && result->v.boolV))
        {

            found = true;
        }
    }

    if(found){
        freeVal(result);
        return RC_OK;
    }else {
        freeVal(result);
        ((RM_ScanMgmt *)scan->mgmtData)->id->page = 1;

        return RC_RM_NO_MORE_TUPLES;
    }
}


extern RC closeScan (RM_ScanHandle *scan)
{
    return RC_OK;
}


extern int getRecordSize(Schema *schema)
{
    int i;
    int result =0;

    for(i = 0; i < schema->numAttr; i++)
    {
        switch (schema->dataTypes[i]){
            case DT_INT:
                result += sizeof(int);
                break;
            case DT_FLOAT:
                result += sizeof(float);
                break;
            case DT_BOOL:
                result += sizeof(bool);
            case DT_STRING:
                result += schema->typeLength[i];
                break;
            default:
                return RC_RM_UNKOWN_DATATYPE;
        }
    }

    return result;
}


extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *schema = (Schema*) calloc(1,sizeof(Schema));

    schema->typeLength = typeLength;
    schema->keyAttrs = keys;
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->keySize = keySize;

    return schema;
}

extern RC freeSchema (Schema *schema)
{

    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->keyAttrs);
    free(schema->typeLength);
    free(schema);

    return RC_OK;
}


extern RC createRecord (Record **record, Schema *schema) {
    if (schema) {
        *record = (Record *) calloc(1, sizeof(Record));

        (*record)->data = (char *) calloc(1, getRecordSize(schema));
        (*record)->id.page = -1;
        (*record)->id.slot = -1;

        return RC_OK;

    }

    return RC_RM_UNKOWN_DATATYPE;
}


extern RC freeRecord (Record *record)
{
    free(record->data);
    free(record);

    return RC_OK;
}

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    int offset;

    attrOffset(schema, attrNum, &offset);

    *value = (Value*)calloc(1,sizeof(Value));
    (*value)->dt =schema->dataTypes[attrNum];

    switch(schema->dataTypes[attrNum])
    {
        case DT_INT:
            memcpy(&((*value)->v.intV) ,record->data + offset,sizeof(int));	//get the attribute into value
            break;
        case DT_STRING:
            (*value)->v.stringV = (char *) calloc(1,(size_t)schema->typeLength[attrNum]);
            strncpy((*value)->v.stringV, record->data + offset, (size_t)schema->typeLength[attrNum]);
            break;
        case DT_FLOAT:
            memcpy(&((*value)->v.floatV),record->data + offset, sizeof(float));
            break;
        case DT_BOOL:
            memcpy(&((*value)->v.boolV),record->data + offset ,sizeof(bool));
            break;
        default:
            return RC_RM_UNKOWN_DATATYPE;
    }
    return RC_OK;
}

extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    int offset;
    attrOffset(schema, attrNum, &offset);

    switch(schema->dataTypes[attrNum])
    {
        case DT_INT:
            memcpy(record->data + offset,&(value->v.intV) ,sizeof(int));
            break;
        case DT_STRING:
            memcpy(record->data + offset,value->v.stringV,(size_t)schema->typeLength[attrNum]);
            break;
        case DT_FLOAT:
            memcpy(record->data + offset,&(value->v.floatV), sizeof(float));
            break;
        case DT_BOOL:
            memcpy(record->data + offset,&(value->v.boolV) ,sizeof(bool));
            break;
        default:
            return RC_RM_UNKOWN_DATATYPE;
    }

    return RC_OK;
}


extern Schema * deserializeSchema(char *serializedSchemaData)
{
    Schema *schema = (Schema*)malloc(sizeof(Schema));

    int numAttr;
    sscanf(serializedSchemaData,"Schema with <%d>",&numAttr);

    schema->numAttr = numAttr;
    schema->attrNames = (char **)malloc(sizeof(char*) * schema->numAttr);
    schema->dataTypes = (DataType*)malloc(sizeof(DataType) * schema->numAttr);
    schema->typeLength = (int*)malloc(sizeof(int) * schema->numAttr);

    strtok(serializedSchemaData,"(");

    int i;
    for(i=0; i < numAttr; i++)
    {
        char *splitEnd = strtok(NULL,": ");

        schema->attrNames[i] = (char*)calloc(1,sizeof(splitEnd));
        strcpy(schema->attrNames[i],splitEnd);

        splitEnd = strtok(NULL,", ");

        switch (splitEnd[0]){
            case 'I':
                schema->dataTypes[i] = DT_INT;
                schema->typeLength[i] = sizeof(int);
                break;
            case 'F':
                schema->dataTypes[i] = DT_FLOAT;
                schema->typeLength[i] = sizeof(float);
                break;
            case 'B':
                schema->dataTypes[i] = DT_BOOL;
                schema->typeLength[i] = sizeof(bool);
                break;
            default:
                schema->dataTypes[i] = DT_STRING;
                int val;
                sscanf(splitEnd,"STRING[%d]",&val);

                schema->typeLength[i] = val;
                break;
        }
    }

    return schema;
}

extern RC deserializeRecord(char *string, Schema *schema, Record ** record)
{
    Value *value;
    createRecord(record,schema);

    strtok(string,"(");

    int i;
    for(i=0;i< schema->numAttr;i++)
    {
        strtok(NULL,":");
        char *splitEnd = strtok(NULL,",");

        switch(schema->dataTypes[i])
        {
            case DT_INT:
                if(splitEnd){
                    MAKE_VALUE(value,DT_INT,atoi(splitEnd));
                }else{
                    MAKE_VALUE(value,DT_INT,0);
                }
                setAttr(*record,schema,i,value);
                break;
            case DT_FLOAT:
                if(splitEnd) {
                    MAKE_VALUE(value, DT_FLOAT, atof(splitEnd));
                }else{
                    MAKE_VALUE(value, DT_FLOAT, 0);
                }
                setAttr(*record,schema,i,value);
                break;
            case DT_BOOL:
                MAKE_VALUE(value,DT_BOOL,(splitEnd[0] == 't') ? TRUE: FALSE);
                setAttr(*record,schema,i,value);
                break;
            case DT_STRING:
                if(splitEnd) {
                    MAKE_STRING_VALUE(value, splitEnd);
                }else{
                    MAKE_STRING_VALUE(value, "");
                }
                setAttr (*record,schema,i,value);
                break;
        }
    }

    return RC_OK;
}

extern RC attrOffset (Schema *schema, int attrNum, int *result)
{
    int offset = 0;
    int attrPos = 0;

    for(attrPos = 0; attrPos < attrNum; attrPos++)
        switch (schema->dataTypes[attrPos])
        {
            case DT_STRING:
                offset += schema->typeLength[attrPos];
                break;
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
        }

    *result = offset;
    return RC_OK;
}