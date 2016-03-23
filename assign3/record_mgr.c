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
    GSchema = schema;

    RM_TableData *tData = (RM_TableData*) malloc (sizeof(RM_TableData));

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


    printf("%s\n",serializeSchema(schema));

    tData->mgmtData = bm;

    //Page 0 store table information

    char *serializedSchema= serializeSchema(tData->schema);
    printf("1\n");
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();

    ph->data= (char*)malloc(sizeof(PAGE_SIZE));
    ph->pageNum=0;

    pinPage(bm,ph,0); //Todo check RC_OK
    memcpy(ph->data,serializedSchema,PAGE_SIZE);

    markDirty(bm,ph); //Todo check RC_OK
    unpinPage(bm,ph); //Todo check RC_OK
    //Closing bufferPool.
    shutdownBufferPool(bm);

    printf("Create table end.\n");
    return RC_OK;
}

int openTable(RM_TableData *rel, char *name) {
    BM_BufferPool *bm = MAKE_POOL();

    int result = initBufferPool(bm,name,3,RS_FIFO,NULL);
    if(result != RC_OK) return result;

    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    ph->data= (char*)malloc(sizeof(PAGE_SIZE));
    ph->pageNum=0;

    pinPage(bm,ph,0); //Todo check RC_OK

    //Todo read schema
    rel->schema = (Schema*) malloc(sizeof(Schema));
    rel->schema =  deserializeSchema(ph->data);
    rel->name = name;

    rel->mgmtData = bm;
    unpinPage(bm,ph);
    printf("Open table end.\n");
    return RC_OK;
}

int closeTable(RM_TableData *rel) {

    shutdownBufferPool(rel->mgmtData);

    freeSchema(rel->schema);

    free(rel);
    return RC_OK;
}

int deleteTable(char *name) {

    return destroyPageFile(name);
}

int getNumTuples(RM_TableData *rel) {
    //Todo Number tuples in table
    int total=0;

    Record *record;

    createRecord(&record,rel->schema);
    RID id;
    id.page=1;
    id.slot=0;

    int totalPages = ((BM_BufferPool*)rel->mgmtData)->numPages;

    while(id.page<totalPages){
        int result = getRecord(rel,id,record);

        if(result == RC_OK) {
            total++;
            id.slot++;
        }
        else {
            id.page++;
            id.slot =0;
        }
    }
    return total;
}

// handling records in a table

int insertRecord(RM_TableData *rel, Record *record) {
    //Todo add new tuple.
    printf("insert record\n");



    return 0;
}

int deleteRecord(RM_TableData *rel,RID id) {
    //Todo delete tuple.
    return 0;
}

int updateRecord(RM_TableData *rel, Record *record) {

    return 0;
}

int getRecord(RM_TableData *rel,RID id,Record *record) {

    BM_PageHandle *bm = MAKE_PAGE_HANDLE();
    int result = pinPage(rel->mgmtData,bm,id.page);

    if(result != RC_OK) return result;

    char *pointer = bm->data+(getRecordSize(rel->schema)*id.slot);
    printf("gR3 = %s,%d\n",bm->data,getRecordSize(rel->schema));


    memcpy(record->data,pointer,getRecordSize(rel->schema));
    record->id=id;
    printf("gR4\n");
    result = unpinPage(rel->mgmtData,bm);

    if(result != RC_OK) return result;
    printf("gR5\n");
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

    printf("n1\n");
    record = (Record *)  malloc(sizeof(record));
    RID *rid = scan->mgmtData;
    getRecord(scan->rel,*rid,record);
    printf("n2\n");
    rid->slot++;

    BM_BufferPool *bm =scan->mgmtData;
    printf("n3\n");

    if(PAGE_SIZE<=rid->slot*getRecordSize(scan->rel->schema)){
        rid->slot=0;
        rid->page++;
    }else
    if(bm->numPages< rid->page){
        return RC_RM_NO_MORE_TUPLES;
    }
    printf("n3.1\n");


    printf("n4\n");
    //Check number of slots.
    return RC_OK;
}

int closeScan(RM_ScanHandle *scan) {
    return 0;
}

// dealing with schemas

int getRecordSize(Schema *schema) {
    //Size of a tuple
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
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->keyAttrs);
    free(schema->typeLength);

    return RC_OK;
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


///////===========

// dynamic string
typedef struct VarString {
    char *buf;
    int size;
    int bufsize;
} VarString;

#define MAKE_VARSTRING(var)				\
		do {							\
			var = (VarString *) malloc(sizeof(VarString));	\
			var->size = 0;					\
			var->bufsize = 100;					\
			var->buf = malloc(100);				\
		} while (0)

#define FREE_VARSTRING(var)			\
		do {						\
			free(var->buf);				\
			free(var);					\
		} while (0)

#define GET_STRING(result, var)			\
		do {						\
			result = malloc((var->size) + 1);		\
			memcpy(result, var->buf, var->size);	\
			result[var->size] = '\0';			\
		} while (0)

#define RETURN_STRING(var)			\
		do {						\
			char *resultStr;				\
			GET_STRING(resultStr, var);			\
			FREE_VARSTRING(var);			\
			return resultStr;				\
		} while (0)

#define ENSURE_SIZE(var,newsize)				\
		do {								\
			if (var->bufsize < newsize)					\
			{								\
				int newbufsize = var->bufsize;				\
				while((newbufsize *= 2) < newsize);			\
				var->buf = realloc(var->buf, newbufsize);			\
			}								\
		} while (0)

#define APPEND_STRING(var,string)					\
		do {									\
			ENSURE_SIZE(var, var->size + strlen(string));			\
			memcpy(var->buf + var->size, string, strlen(string));		\
			var->size += strlen(string);					\
		} while(0)

#define APPEND(var, ...)			\
		do {						\
			char *tmp = malloc(10000);			\
			sprintf(tmp, __VA_ARGS__);			\
			APPEND_STRING(var,tmp);			\
			free(tmp);					\
		} while(0)

/*
 * Function to deserialize the Schema
 */
Schema *deserializeSchema(char *serializedSchemaData)
{
    int i,j;
    VarString *result;
    MAKE_VARSTRING(result);

    Schema *schema = (Schema*)malloc(sizeof(Schema));

    int schemaNumAttr,lastAttr;

    char *splitStart = (char*)malloc(sizeof(char));
    char *splitEnd = (char*)malloc(sizeof(char));
    char *splitString = (char*)malloc(sizeof(char));

    //split on token values
    splitStart = strtok(serializedSchemaData,"<");
    splitEnd = strtok(NULL,">");

    //convert to long/int values
    schemaNumAttr = strtol(splitEnd, &splitStart,10);

    schema->numAttr = schemaNumAttr;

    schema->attrNames = (char **)malloc(sizeof(char*) * schemaNumAttr);
    schema->dataTypes = (DataType*)malloc(sizeof(DataType) * schemaNumAttr);
    schema->typeLength = (int*)malloc(sizeof(int) * schemaNumAttr);

    splitEnd = strtok(NULL,"(");

    lastAttr =  schemaNumAttr-1;

    //put in the dataTypes and thier datalengths
    for(i=0;i<schemaNumAttr;i++)
    {
        splitEnd = strtok(NULL,": ");

        schema->attrNames[i] = (char*)malloc(sizeof(char*));
        strcpy(schema->attrNames[i],splitEnd);

        if(i == lastAttr)
        {
            splitEnd = strtok(NULL,") ");
        }
        else
        {
            splitEnd = strtok(NULL,", ");
        }

        if(strcmp(splitEnd,"INT")==0)
        {
            schema->dataTypes[i] = DT_INT;
            schema->typeLength[i] = 0;
        }
        else if(strcmp(splitEnd,"FLOAT")==0)
        {
            schema->dataTypes[i] = DT_FLOAT;
            schema->typeLength[i] = 0;
        }
        else if(strcmp(splitEnd,"BOOL")==0)
        {
            schema->dataTypes[i] = DT_BOOL;
            schema->typeLength[i] = 0;
        }
        else
        {
            strcpy(splitString, splitEnd);
            char *str = (char*)malloc(sizeof(char));
            sprintf(str,"%d",i);
            strcat(splitString,str);

            str = NULL;
            free(str);
        }
    }//end for()

    //put in the keyAttrs

    //check if there are any keys present
    if((splitEnd = strtok(NULL,"("))!=NULL)
    {
        splitEnd = strtok(NULL,")");
        char *splitKey = (char*)malloc(sizeof(char));
        char *keyAttr[schemaNumAttr];
        int numOfKeys = 0;

        splitKey = strtok(splitEnd,", ");

        //Find out the number of Keys & store the attrValues for those Keys
        while(splitKey!=NULL)
        {
            keyAttr[numOfKeys] = (char*)malloc(sizeof(char*));
            strcpy(keyAttr[numOfKeys],splitKey);
            numOfKeys++;
            splitKey = strtok(NULL,", ");
        }

        splitKey = NULL;
        free(splitKey);

        //MARK all the key attrs as their INDEX values
        schema->keyAttrs = (int*)malloc(sizeof(int)*numOfKeys);
        schema->keySize = numOfKeys;

        //for every Key, find the attributes and mark it's index
        for(i=0;i<numOfKeys;i++)
        {
            for(j=0;j<schemaNumAttr;j++)
            {
                if(strcmp(keyAttr[i],schema->attrNames[j])==0)
                {
                    //mark the index
                    schema->keyAttrs[i] = j;
                    break;
                }
            }
        }
    }

    //for STRING[SIZE] allocate all the attributes

    if(strlen(splitString)!=0)
    {
        splitString = strtok(splitString,"[");
        if(strcmp(splitString,"STRING")==0)
        {
            int val, index;
            splitString = strtok(NULL,"]");
            val = atoi(splitString);
            splitString = strtok(NULL,"=");
            index = atoi(splitString);
            schema->dataTypes[index] = DT_STRING;

            schema->typeLength[index] = val;
        }
    }

    splitString = NULL;
    splitEnd = NULL;
    splitStart = NULL;
    free(splitString);
    free(splitStart);
    free(splitEnd);

    return schema;
}

/*
 * Deserialize Record - split into tokens
 */
Record*
deserializeRecord(char *desiralize_record_str, Schema *schema)
{
    int i, lastAttr = schema->numAttr-1;
    int intVal;
    float floatVal;
    bool boolVal;

    Value *value;
    Record *record = (Record*)malloc(sizeof(Record*));
    record->data = (char*)malloc(sizeof(char*));

    char *splitStart, *splitEnd;

    splitStart = strtok(desiralize_record_str,"(");

    for(i=0;i< schema->numAttr;i++)
    {
        splitEnd = strtok(NULL,":");

        if(i == lastAttr)
        {
            splitEnd = strtok(NULL,")");
        }
        else
        {
            splitEnd = strtok(NULL,",");
        }

        switch(schema->dataTypes[i])
        {
            case DT_INT:
                intVal = strtol(splitEnd, &splitStart, 10);
                MAKE_VALUE(value,DT_INT,intVal);
                setAttr(record,schema,i,value);
                free(value);
                break;

            case DT_FLOAT:
                floatVal = strtof(splitEnd, NULL);
                MAKE_VALUE(value,DT_FLOAT,floatVal);
                setAttr(record,schema,i,value);
                free(value);
                break;

            case DT_BOOL:
                boolVal = (splitEnd[0] == 't') ? TRUE: FALSE;
                MAKE_VALUE(value,DT_BOOL,boolVal);
                setAttr(record,schema,i,value);
                free(value);
                break;

            case DT_STRING:
                MAKE_STRING_VALUE(value,splitEnd);
                setAttr (record,schema,i,value);
                freeVal(value);
                break;
        }

    }

    return record;
}