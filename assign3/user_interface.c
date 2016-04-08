
#include <stdlib.h>
#include "record_mgr.h"
#include "string.h"

Schema *
createSchemaUI ();
// main method
int
main (void)
{
    initRecordManager(NULL);

    char* name;
    printf("Open/Create table: ");
    scanf("%s",name);

    RM_TableData *rel = (RM_TableData *) malloc(sizeof(RM_TableData));
    Schema *schema;

    if(openTable(rel,name) != RC_OK){
        printf("Create New table:\n");
        printf("New Table with the schema:\n %s\n",serializeSchema(createSchemaUI()));
        openTable(rel,name);
    }
    printf("open table [%s]\n",name);
    do {
        printf("Commands \n");
        printf("--------\n");
        printf("1 Insert\n");
        int command;
        scanf("%d", &command);

        switch (command) {
            case 1:
                break;
            default:
                printf("Command not found\n");
        }
    }while(0);
}

void insertRecordUI(){

}

Schema *
createSchemaUI ()
{
    int col = 0;
    do {
        printf("Number of columns: ");
        scanf("%d", &col);
    }while(col <=0);

    printf("Create Columns names\n");
    printf("--------------------\n");
    char **names =(char **) malloc(sizeof(char*) * col);
    int i;
    for(i=0;i<col;i++){
        printf("Name of column %d: ",i);
        char *tmp = calloc(1,10);
        scanf("%s",tmp);
        names[i] = calloc(1,sizeof(tmp));
        strcpy(names[i],tmp);
    }

    printf("Set column types\n");
    printf("--------------------\n");
    printf("1-DT_INT\n");
    printf("2-DT_FLOAT\n");
    printf("3-DT_BOOL\n");
    printf("4-DT_STRING\n");
    DataType dt[col];

    for(i=0;i<col;i++){
        int temp;
        do {
            printf("Type of column %d[%s]: ",i,names[i]);
            scanf("%d", &temp);
        }while(temp <0 && temp > 4);
        switch (temp){
            case 1:
                dt[i] = DT_INT;
                break;
            case 2:
                dt[i] = DT_FLOAT;
                break;
            case 3:
                dt[i] = DT_BOOL;
                break;
            case 4:
                dt[i] = DT_STRING;
                break;
        }
    }

    printf("Set Sizes\n");
    printf("--------------------\n");

    int sizes[col];
    for(i=0;i<col;i++){
        sizes[i]=0;
        if(dt[i] == DT_STRING){
            int temp;
            do {
                printf("Size of column %d[%s][DT_STRING]: ",i,names[i]);
                scanf("%d", &temp);
            }while(temp <0 && temp > 10);
            sizes[i]=temp;
        }

    }


    int keys[] = {0};

    char **cpNames = (char **) malloc(sizeof(char*) * 3);
    DataType *cpDt = (DataType *) malloc(sizeof(DataType) * 3);
    int *cpSizes = (int *) malloc(sizeof(int) * 3);
    int *cpKeys = (int *) malloc(sizeof(int));

    for(i = 0; i < col; i++)
    {
        cpNames[i] = (char *) malloc(2);
        strcpy(cpNames[i], names[i]);
    }
    memcpy(cpDt, dt, sizeof(DataType) * 3);
    memcpy(cpSizes, sizes, sizeof(int) * 3);
    memcpy(cpKeys, keys, sizeof(int));

    Schema *result;
    result = createSchema(col, cpNames, cpDt, cpSizes, 1, cpKeys);

    return result;
}
