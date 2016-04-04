//
// Created by drow on 3/04/16.
//

#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"

typedef struct RM_RecordMgmt
{
    BM_BufferPool *bm;
    int *freePages;
}RM_RecordMgmt;

// table and manager
extern RC initRecordManager (void *mgmtData){
    return RC_OK;
}

extern RC shutdownRecordManager (){
    return RC_OK;
}

RC createTable (char *name, Schema *schema){
    printf("1\n");

    //creating page file
    initStorageManager();
    int result = createPageFile(name);

    if(result != RC_OK) return result;

    //Init buffer pool.
    BM_BufferPool *bm = MAKE_POOL();

    result = initBufferPool(bm,name,3,RS_FIFO,NULL);
    if(result != RC_OK) return result;
    /*
    if(schema == NULL){
        printf("Schema is Null\n");
        schema = (Schema *) malloc(sizeof(Schema));
    }
*/
    printf("schema:\n");
    printf("schema: %d\n",schema->numAttr);
    //Page 0 store table information
    char *serializedSchema= serializeSchema(schema);

    printf("Schema:\n %s\n",serializedSchema);
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();

    ph->data= (char*)malloc(sizeof(PAGE_SIZE));
    ph->pageNum=0;

    pinPage(bm,ph,0); //Todo check RC_OK
    memcpy(ph->data,serializedSchema,PAGE_SIZE);
    markDirty(bm,ph); //Todo check RC_OK
    unpinPage(bm,ph); //Todo check RC_OK
    //Closing bufferPool.
    shutdownBufferPool(bm);

    free(ph->data);
    free(ph);

    return RC_OK;
}

extern RC openTable (RM_TableData *rel, char *name){
    RM_RecordMgmt *rm_mgmt = (RM_RecordMgmt *)malloc(sizeof(RM_RecordMgmt));

    BM_BufferPool *bm = MAKE_POOL();

    int result = initBufferPool(bm,name,3,RS_FIFO,NULL);
    if(result != RC_OK) return result;
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    ph->data= (char*)malloc(sizeof(PAGE_SIZE));
    ph->pageNum=0;


    result =pinPage(bm,ph,0);
    if(result != RC_OK) return result;

    //Todo read schema
    rel->schema = (Schema*) malloc(sizeof(Schema));

    rel->schema =  deserializeSchema(ph->data);
    rel->name = name;
    rel->mgmtData = rm_mgmt;

    result = unpinPage(bm,ph);
    if(result != RC_OK) return result;

    printf("Schema: \n%s\n",serializeSchema(rel->schema));

    rm_mgmt->bm=bm;
    return RC_OK;
}

extern RC closeTable (RM_TableData *rel){

    shutdownBufferPool(((RM_RecordMgmt*)rel->mgmtData)->bm);
    freeSchema(rel->schema);

    return RC_OK;
}

extern RC deleteTable (char *name){
    return destroyPageFile(name);
}

extern RC deleteRecord (RM_TableData *rel, RID id){
    //Todo all
    return RC_OK;
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

    freeRecord(record);
    return total;
}


// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){

    //Todo add new tuple.

    printf("Schema: %s\n",serializeSchema(rel->schema));
    printf("insert record %s\n",serializeRecord(record,rel->schema));

    int totalPages = ((BM_BufferPool*)rel->mgmtData)->numPages;
    int recordSize = getRecordSize(rel->schema);
    int recordsPerPage = PAGE_SIZE/recordSize;

    int result = RC_OK;

    //Find the next place of insertion of a record
    RID searchRid;
    searchRid.slot=0;
    searchRid.page=1;
    Record *record1;
    result = createRecord(&record1,rel->schema);
    if(result != RC_OK) return result;

    while(record->id.slot < 0 && searchRid.page < totalPages){
        while(record->id.slot < 0 && searchRid.slot< recordsPerPage){

            if(getRecord(rel,searchRid,record1)!=RC_OK){
                printf("Found where to locate %d\n",searchRid.slot);
                record->id.slot=searchRid.slot;
                record->id.page=searchRid.page;
            }else{
                searchRid.slot++;
            }

        }
        searchRid.slot=0;
        searchRid.page++;
    }

    BM_PageHandle *page = MAKE_PAGE_HANDLE();

    char * serializedRecord = serializeRecord(record, rel->schema);

    result = pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, record->id.page);
    if(result != RC_OK) return result;

    //Move pointer to the slot
    memset(page->data, '\0',(size_t) recordSize*record->id.slot);
    sprintf(page->data, "%s", serializedRecord);

    result = markDirty(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
    if(result != RC_OK) return result;

    forcePage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

    result = unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
    if(result != RC_OK) return result;

    printf("Inserted\n");

    result = freeRecord(record1);
    if(result != RC_OK) return result;

    return RC_OK;
}

extern RC updateRecord (RM_TableData *rel, Record *record){
//Find the data to be updated
    int totalPages = ((BM_BufferPool*)rel->mgmtData)->numPages;
    if(record->id.page > 0 && record->id.page <=  totalPages)
    {
        BM_PageHandle *page = MAKE_PAGE_HANDLE();

        int pageNum, slotNum;

        // Setting record id and slot number
        pageNum = record->id.page;
        slotNum = record->id.slot;

        //Compare if the record is a deleted Record,
        //return update not possible for deleted records (EC 401)

        if(strncmp(record->data, "DELETED_RECORD", 14) == 0)
            return RC_RM_UPDATE_NOT_POSSIBLE_ON_DELETED_RECORD;

        //Take the serailized updated record data
        char *record_str = serializeRecord(record, rel->schema);

        //pin page
        pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, record->id.page);

        //set the new fields, or the entire modified page
        memset(page->data, '\0', strlen(page->data));
        sprintf(page->data, "%s", record_str);

        //free the temp data
        free(record_str);

        //mark the page as dirty
        markDirty(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

        //unpin the page, after use
        unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

        //force write onto the page, as it is modified page now
        forcePage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

        //printf("record data in update: %s\n", page->data);

        free(page);		//free page, avoid leaks
        return RC_OK;
    }
    else
    {
        return RC_RM_NO_MORE_TUPLES;	//return the data to be modfied not found
    }
}

extern RC getRecord (RM_TableData *rel, RID id, Record *record){

    int recordSize = getRecordSize(rel->schema);
    int result = RC_OK;

    printf("GR\n");
    //find the record in the record table
    int totalPages = ((BM_BufferPool*)rel->mgmtData)->numPages;
    if(id.page > 0 && id.page <=  totalPages)
    {
        printf("Init getRecord\n");
        //make a page handle
        BM_PageHandle *page = MAKE_PAGE_HANDLE();
        printf("Init pin page\n");
        //pin page, and mark it for use
        result = pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, id.page);
        printf("end pin page\n");
        if(result != RC_OK) return result;

        printf("GR1 len: %d\n",(int)strlen(page->data));
        if(strlen(page->data) <= 0){
            printf("Page data is: \n");
            return RC_RM_NO_MORE_TUPLES;
        }
        printf("GR1.1\n");
        //temp, to store the record data
        char *record_data = (char*)malloc(sizeof(char) * strlen(page->data));
        printf("GR1.2 %d, %d, %d\n",recordSize*id.slot,recordSize,id.slot);

        //copy the data

        memset(page->data, '\0',(size_t) recordSize*id.slot);
        printf("GR1.3\n");
        strncpy(record_data,page->data,(size_t) recordSize);
        printf("Record Stored: %s\n",record_data);

        //store the record data and id
        record->id = id;
        printf("GR2 ,%s\n",record_data);

        //deSerialze the data
        result = deserializeRecord(record_data,rel->schema,record);
        if(result != RC_OK) return result;
        //free(record_data);

        printf("GR2.1\n");
        //unpin the page, after fetching the record
        result = unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
        if(result != RC_OK) return result;

        printf("GR2.2\n");
        //return the new data

        printf("GR3\n");
        //printf("Record Data in getRecord: %s\n",record->data);

        //free temp. allocations to avoid memory leaks

        free(page);
        printf("GR4\n");
        return RC_OK;
    }
    else		//if record not found return RC_RM_NO_MORE_TUPLES
    {
        return RC_RM_NO_MORE_TUPLES;
    }
}

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    //Todo
    return RC_OK;
}

extern RC next (RM_ScanHandle *scan, Record *record){
    //Todo
    return RC_OK;
}

extern RC closeScan (RM_ScanHandle *scan){
    //Todo
    return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema){
    //Todo Change
    int i=0;

    //10 size for position
    int recordSize = 10;

    for(i = 0; i < schema->numAttr; i++)
    {
        if(schema->dataTypes[i] == DT_INT){
            recordSize += sizeof(int);
            //Name plus dots
            recordSize += (strlen(schema->attrNames[i])+1);
        }


        else if(schema->dataTypes[i] == DT_FLOAT) {
            recordSize += sizeof(float);
            //Name plus dots
            recordSize += (strlen(schema->attrNames[i])+1);
        }
        else if(schema->dataTypes[i] == DT_BOOL) {
            recordSize += sizeof(bool);
            //Name plus dots
            recordSize += (strlen(schema->attrNames[i]) + 1);
        }
        else{
            //Name plus dots
            recordSize += (strlen(schema->attrNames[i])+1);
            recordSize += schema->typeLength[i];
        }

    }
    return recordSize;
}
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){

    Schema *schema = (Schema*)malloc(sizeof(Schema));

    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;

    return schema;
}
extern RC freeSchema (Schema *schema){
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->keyAttrs);
    free(schema->typeLength);

    return RC_OK;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema){

    if(*record){
        printf("-------- Not Null\n");
        *record = (Record*) malloc (sizeof(Record));

        //No allocated
        (*record)->id.page=-1;
        (*record)->id.slot=-1;
        //Default Size
        (*record)->data = (char*)malloc(getRecordSize(schema));

        return RC_OK;
    }else{
        printf("-------- Null\n");
        Record *aux = (Record*) malloc(sizeof(Record));
        printf("-------- Null\n");
        record = &aux;

        //No allocated
        (*record)->id.page=-1;
        (*record)->id.slot=-1;
        //Default Size
        (*record)->data = (char*)malloc(getRecordSize(schema));

        return RC_OK;
    }

}

extern RC freeRecord (Record *record){
    //Todo
    printf("FreeRecord\n");
    //free(record->id.page);
    //free(record->id.slot);
    //free(record->data);
    free(record);
    return RC_OK;
}

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    //Todo
    return RC_OK;
}

extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    //Todo
    return RC_OK;
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

    schema->keySize = 0;

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
        while(splitKey!=NULL && (strlen(splitKey) != 0&& splitKey[0] !='\n'))
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
RC deserializeRecord(char *string, Schema *schema, Record *record)
{
    printf("deserializeRecord\n");
    printf("befour read deserializeRecord\n");
    scanf("[%d-%d] %s", &record->id.page,&record->id.slot,record->data);

    printf("4\n");
    return RC_OK;
}
