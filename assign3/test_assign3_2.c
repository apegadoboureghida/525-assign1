#include <stdlib.h>
#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "tables.h"
#include "test_helper.h"


#define ASSERT_EQUALS_RECORDS(_l,_r, schema, message)			\
  do {									\
    Record *_lR = _l;                                                   \
    Record *_rR = _r;                                                   \
    ASSERT_TRUE(memcmp(_lR->data,_rR->data,getRecordSize(schema)) == 0, message); \
    int i;								\
    for(i = 0; i < schema->numAttr; i++)				\
      {									\
        Value *lVal, *rVal;                                             \
		char *lSer, *rSer; \
        getAttr(_lR, schema, i, &lVal);                                  \
        getAttr(_rR, schema, i, &rVal);                                  \
		lSer = serializeValue(lVal); \
		rSer = serializeValue(rVal); \
        ASSERT_EQUALS_STRING(lSer, rSer, "attr same");	\
		free(lVal); \
		free(rVal); \
		free(lSer); \
		free(rSer); \
      }									\
  } while(0)

#define ASSERT_EQUALS_RECORD_IN(_l,_r, rSize, schema, message)		\
  do {									\
    int i;								\
    boolean found = false;						\
    for(i = 0; i < rSize; i++)						\
      if (memcmp(_l->data,_r[i]->data,getRecordSize(schema)) == 0)	\
	found = true;							\
    ASSERT_TRUE(0, message);						\
  } while(0)

#define OP_TRUE(left, right, op, message)		\
  do {							\
    Value *result = (Value *) malloc(sizeof(Value));	\
    op(left, right, result);				\
    bool b = result->v.boolV;				\
    free(result);					\
    ASSERT_TRUE(b,message);				\
   } while (0)

// test methods
static void testDeleteReAllocation (void);
static void testCloseNotOpenTable(void);
static void testCreateRecordWithoutSchema(void);

// struct for test records
typedef struct TestRecord {
    int a;
    char *b;
    int c;
} TestRecord;

// helper methods
Record *testRecord(Schema *schema, int a, char *b, int c);
Schema *testSchema (void);
Record *fromTestRecord (Schema *schema, TestRecord in);

// test name
char *testName;

// main method
int
main (void)
{
  testName = "";

  testDeleteReAllocation();
  testCloseNotOpenTable();
  testCreateRecordWithoutSchema();
  return 0;
}



Schema *
testSchema (void)
{
  Schema *result;
  char *names[] = { "a", "b", "c" };
  DataType dt[] = { DT_INT, DT_STRING, DT_INT };
  int sizes[] = { 0, 4, 0 };
  int keys[] = {0};
  int i;
  char **cpNames = (char **) malloc(sizeof(char*) * 3);
  DataType *cpDt = (DataType *) malloc(sizeof(DataType) * 3);
  int *cpSizes = (int *) malloc(sizeof(int) * 3);
  int *cpKeys = (int *) malloc(sizeof(int));

  for(i = 0; i < 3; i++)
  {
    cpNames[i] = (char *) malloc(2);
    strcpy(cpNames[i], names[i]);
  }
  memcpy(cpDt, dt, sizeof(DataType) * 3);
  memcpy(cpSizes, sizes, sizeof(int) * 3);
  memcpy(cpKeys, keys, sizeof(int));

  result = createSchema(3, cpNames, cpDt, cpSizes, 1, cpKeys);

  return result;
}

Record *
fromTestRecord (Schema *schema, TestRecord in)
{
  return testRecord(schema, in.a, in.b, in.c);
}

Record *
testRecord(Schema *schema, int a, char *b, int c)
{
  Record *result;
  Value *value;

  TEST_CHECK(createRecord(&result, schema));

  MAKE_VALUE(value, DT_INT, a);
  TEST_CHECK(setAttr(result, schema, 0, value));
  freeVal(value);

  MAKE_STRING_VALUE(value, b);
  TEST_CHECK(setAttr(result, schema, 1, value));
  freeVal(value);

  MAKE_VALUE(value, DT_INT, c);
  TEST_CHECK(setAttr(result, schema, 2, value));
  freeVal(value);

  return result;
}

// ************************************************************
void
testDeleteReAllocation(void)
{
  RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
  TestRecord inserts[] = {
          {1, "aaaa", 3},
          {2, "bbbb", 2},
          {3, "cccc", 4},
  };
  int numInserts = 9, i;
  Record *r;
  RID *rids;
  Schema *schema;
  testName = "test delete first record and allocate new one in that slot.";
  schema = testSchema();
  rids = (RID *) malloc(sizeof(RID) * numInserts);

  TEST_CHECK(initRecordManager(NULL));
  TEST_CHECK(createTable("test_table_r",schema));
  TEST_CHECK(openTable(table, "test_table_r"));

  r = fromTestRecord(schema, inserts[0]);
  TEST_CHECK(insertRecord(table,r));
  rids[0] = r->id;

  r = fromTestRecord(schema, inserts[1]);
  TEST_CHECK(insertRecord(table,r));
  rids[1] = r->id;

  TEST_CHECK(deleteRecord(table,r[0].id));
  r = fromTestRecord(schema, inserts[2]);
  TEST_CHECK(insertRecord(table,r));
  rids[2] = r->id;
  TEST_CHECK(rids[0].page==rids[2].page&&rids[0].slot==rids[2].slot);

  TEST_CHECK(closeTable(table));
  TEST_CHECK(deleteTable("test_table_r"));
  TEST_CHECK(shutdownRecordManager());

  free(rids);
  free(table);
  TEST_DONE();
}

void
testCloseNotOpenTable(void)
{
  RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
  TestRecord inserts[] = {
          {1, "aaaa", 3},
          {2, "bbbb", 2},
          {3, "cccc", 4},
  };
  int numInserts = 9, i;
  Record *r;
  RID *rids;
  Schema *schema;
  testName = "test Close table that is not open";
  schema = testSchema();
  rids = (RID *) malloc(sizeof(RID) * numInserts);

  TEST_CHECK(initRecordManager(NULL));
  TEST_CHECK(createTable("test_table_r",schema));


  TEST_CHECK(closeTable(table));
  TEST_CHECK(deleteTable("test_table_r"));
  TEST_CHECK(shutdownRecordManager());

  free(rids);
  free(table);
  TEST_DONE();
}

void
testCreateRecordWithoutSchema(void)
{
  RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
  TestRecord inserts[] = {
          {1, "aaaa", 3},
          {2, "bbbb", 2},
          {3, "cccc", 4},
  };
  int numInserts = 9, i;
  Record *r;
  RID *rids;
  Schema *schema;
  testName = "test Close table that is not open";
  schema = testSchema();
  rids = (RID *) malloc(sizeof(RID) * numInserts);

  TEST_CHECK(initRecordManager(NULL));
  TEST_CHECK(createTable("test_table_r",schema));
  TEST_CHECK(openTable(table, "test_table_r"));

  //Create record Without Schema
  Record *result;
  TEST_CHECK(!createRecord(&result, NULL));

  TEST_CHECK(closeTable(table));
  TEST_CHECK(deleteTable("test_table_r"));
  TEST_CHECK(shutdownRecordManager());

  free(rids);
  free(table);
  TEST_DONE();
}