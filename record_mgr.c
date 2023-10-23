#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"


RecordManager *recordManager;


extern RC initRecordManager(void *mgmtData)
{
	RC return_code = RC_OK;
	initStorageManager();

	return return_code;
}

extern RC shutdownRecordManager()
{
	RC return_code = RC_OK;

	if (recordManager != NULL)
	{
		free(recordManager);
	}
	recordManager = NULL;

	return return_code;
}

extern RC freeSchema(Schema *schema)
{
	RC return_code = RC_OK;
	
	if (schema != NULL){
		free(schema);
	}
	schema = NULL;

	return return_code;
}

extern RC freeRecord(Record *record)
{
	RC return_code = RC_OK;
	if (record != NULL){
		free(record);
	}
	record = NULL;
	
	return return_code;
}

extern RC closeTable(RM_TableData *rel)
{
	RC return_code = RC_OK;

	RecordManager *recordManager = rel->mgmtData;

	int didShutDown = shutdownBufferPool(&recordManager->bufferPool);

	return return_code;
}

extern RC deleteTable(char *name)
{
	RC return_code = RC_OK;
	
	int destroyed = destroyPageFile(name);

	return return_code;
}

extern int getNumTuples(RM_TableData *rel)
{
	int numTuples = recordManager->attrCount;
	return numTuples;
}

extern RC operateFile(char *fileName, SM_FileHandle sm_fileHandle, char data[]){
	RC return_code = RC_OK;
	int isFileCreated = createPageFile(fileName);
	if (isFileCreated != RC_OK)
	{
		return_code = RC_FILE_HANDLE_NOT_INIT;
		printError(return_code);

		return return_code;
	}

	int isFileOpened = openPageFile(fileName, &sm_fileHandle);
	if (isFileOpened != RC_OK)
	{		
		return_code = RC_FILE_NOT_FOUND;
		printError(return_code);

		return return_code;
	}

	int isFileWrite = writeBlock(0, &sm_fileHandle, data);

	if (isFileWrite != RC_OK)
	{
		return_code = RC_WRITE_FAILED;
		printError(return_code);

		return return_code;
	}

	return return_code;
}

extern RC createTable(char *name, Schema *schema)
{
	RC return_code = RC_OK;
	recordManager = (RecordManager *)malloc(sizeof(RecordManager));

	initBufferPool(&recordManager->bufferPool, name, 100, RS_CLOCK, NULL);

	char data[PAGE_SIZE];
	char *handler = data;
	SM_FileHandle sm_fileHandle;

	int values[] = {0, 1, schema->numAttr, schema->keySize};
	int* pointer = (int*)handler;
	int range = sizeof(values) / sizeof(values[0]);

	for (int i = 0; i < range; i++) {
		pointer[i] = values[i];
		handler += sizeof(int);
	}

	for (int i = 0; i < schema->numAttr; i++) {
		strncpy(handler, schema->attrNames[i], 15);
		handler += 15;

		int* pointer = (int*)handler;
		pointer[0] = (int)schema->dataTypes[i];
		pointer[1] = (int)schema->typeLength[i];
		handler += 2 * sizeof(int);
	}

	operateFile(name, sm_fileHandle, data);

	closePageFile(&sm_fileHandle);

	return return_code;
}


extern RC openTable(RM_TableData *rel, char *name) {
    RC return_code = RC_OK;

    // Set the management data and name
    rel->mgmtData = recordManager;
    rel->name = name;

    // Read data from the page
    pinPage(&recordManager->bufferPool, &recordManager->bm_pageHandle, 0);
    SM_PageHandle sm_pageHandle = (char *)recordManager->bm_pageHandle.data;

    recordManager->attrCount = *(int *)sm_pageHandle;
    sm_pageHandle += sizeof(int);
    recordManager->freeIndex = *(int *)sm_pageHandle;
    sm_pageHandle += sizeof(int);

    int attributeCount = *(int *)sm_pageHandle;
    sm_pageHandle += sizeof(int);

    // Create arrays to store attribute details
    char **attrNames = (char **)malloc(sizeof(char *) * attributeCount);
    DataType *dataTypes = (DataType *)malloc(sizeof(DataType) * attributeCount);
    int *typeLength = (int *)malloc(sizeof(int) * attributeCount);

    // Loop through attribute details and copy them
    for (int k = 0; k < attributeCount; k++) {
        attrNames[k] = strndup(sm_pageHandle, 15);
        sm_pageHandle += 15;
        dataTypes[k] = *(int *)sm_pageHandle;
        sm_pageHandle += sizeof(int);
        typeLength[k] = *(int *)sm_pageHandle;
        sm_pageHandle += sizeof(int);
    }

    // Create a schema and set it to the table data
    Schema *schema = createSchema(attributeCount, attrNames, dataTypes, typeLength, 0, NULL);
    rel->schema = schema;

    // Cleanup and unpin page
    unpinPage(&recordManager->bufferPool, &recordManager->bm_pageHandle);
    forcePage(&recordManager->bufferPool, &recordManager->bm_pageHandle);

    return return_code;
}

//!Change this
int findFreeSlot(char *data, int totalSlots, int recordSize)
{
    for (int i = 0; i < totalSlots; i++)
    {
        char firstChar = data[i * recordSize];
        if (firstChar != '+')
        {
            return i;
        }
    }
    return -1;
}


extern RC insertRecord(RM_TableData *rel, Record *record)
{
    RC return_code = RC_OK;
    RecordManager *recordManager = rel->mgmtData;
    RID *rid = &record->id;
    char *data;
    int size = getRecordSize(rel->schema);
    int totalSlots = PAGE_SIZE / size;

    pinPage(&recordManager->bufferPool, &recordManager->bm_pageHandle, recordManager->freeIndex);

    rid->page = recordManager->freeIndex;
    data = recordManager->bm_pageHandle.data;

    rid->slot = findFreeSlot(data, totalSlots, size);

    while (rid->slot == -1)
    {
        unpinPage(&recordManager->bufferPool, &recordManager->bm_pageHandle);
        pinPage(&recordManager->bufferPool, &recordManager->bm_pageHandle, rid->page + 1);
        data = recordManager->bm_pageHandle.data;
        
        rid->page += 1;
        rid->slot = findFreeSlot(data, totalSlots, size);
    }

    int slotIndex = rid->slot;

    char *slotPointer = data + (slotIndex * size);
    markDirty(&recordManager->bufferPool, &recordManager->bm_pageHandle);

    *slotPointer = '+';
    memcpy(slotPointer + 1, record->data + 1, size - 1);

    unpinPage(&recordManager->bufferPool, &recordManager->bm_pageHandle);
    recordManager->attrCount++;
    pinPage(&recordManager->bufferPool, &recordManager->bm_pageHandle, 0);

    return return_code;
}


extern RC deleteRecord(RM_TableData *rel, RID id)
{
    RC return_code = RC_OK;
    RecordManager *recordManager = rel->mgmtData;

    // Pin the page containing the record to delete
    int pinnedPage = pinPage(&recordManager->bufferPool, &recordManager->bm_pageHandle, id.page);

    // Update the freePage in the RecordManager
    recordManager->freeIndex = id.page;
    int size = getRecordSize(rel->schema);

    // Calculate the position of the record to delete
    int slotID = id.slot;
    int position = slotID * size;

    recordManager->bm_pageHandle.data += position;

    // Mark the page as dirty
    int isDirty = markDirty(&recordManager->bufferPool, &recordManager->bm_pageHandle);
	if (isDirty != RC_OK){
		return_code = isDirty;
		printError(return_code);

		return return_code;
	}

    // Unpin the page
    unpinPage(&recordManager->bufferPool, &recordManager->bm_pageHandle);

    return return_code;
}


extern RC updateRecord(RM_TableData *rel, Record *record)
{
	RC return_code = RC_OK;
	RecordManager *recordManager = rel->mgmtData;

    int pageNum = record->id.page;
	pinPage(&recordManager->bufferPool, &recordManager->bm_pageHandle, pageNum);

	char *data = recordManager->bm_pageHandle.data;
	int size = getRecordSize(rel->schema);
	RID rid = record->id;

    int slotID = rid.slot;
    int position = slotID * size;

	data += position;
	data[0] = '+';

	memcpy(data + 1, record->data + 1, size - 1);

	markDirty(&recordManager->bufferPool, &recordManager->bm_pageHandle);
	unpinPage(&recordManager->bufferPool, &recordManager->bm_pageHandle);

	return return_code;
}


extern RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    RC return_code = RC_OK;
    RecordManager *recordManager = rel->mgmtData;
    BM_BufferPool *bufferPool = &recordManager->bufferPool;
    BM_PageHandle *bm_pageHandle = &recordManager->bm_pageHandle;
    int pageNum = id.page;
    int slotID = id.slot;
    int size = getRecordSize(rel->schema);

    if (pinPage(bufferPool, bm_pageHandle, pageNum) != RC_OK) {
        return RC_RID_DOES_NOT_EXISTS;
    }

    char *dataPointer = bm_pageHandle->data + (slotID * size);

    if (*dataPointer != '+') {
        unpinPage(bufferPool, bm_pageHandle);
        return RC_RID_DOES_NOT_EXISTS;
    }

    record->id = id;
    memcpy(record->data + 1, dataPointer + 1, getRecordSize(rel->schema) - 1);
    
    unpinPage(bufferPool, bm_pageHandle);
    
    return return_code;
}


extern void setScanManagerAttributes(RecordManager *scanner, Expr *condition){
	scanner->recordID.page = 1;
	scanner->recordID.slot = 0;
	scanner->scanCount = 0;
	scanner->cond = condition;
}


extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	RC return_code = RC_OK;
	openTable(rel, "RecordTable");

	RecordManager *scanner = (RecordManager *)malloc(sizeof(RecordManager));
	RecordManager *recordManager = rel->mgmtData;
	recordManager->attrCount = 15;

	scan->mgmtData = scanner;
	scan->rel = rel;

    setScanManagerAttributes(scanner, cond);

	return return_code;
}


bool conditionExists(RM_ScanHandle *scan) {
    RecordManager *scanManager = (RecordManager *)scan->mgmtData;
    return scanManager->cond != NULL;
}

// Helper function to initialize a record from page data
void initializeRecordFromData(Record *record, char *data, int recordSize, RID *recordID) {
    record->id.page = recordID->page;
    record->id.slot = recordID->slot;

    // Set the first character directly
    record->data[0] = '-';

    // Use memcpy to copy the rest of the data
    memcpy(record->data + 1, data + 1, recordSize - 1);
}

extern RC findNextTuple(int scanCount, int attrCount, RecordManager *scanner, int totalSlots, int size, Record *record, Schema *schema, Value *result){
    while (scanCount < attrCount) {
        if (scanCount <= 0) {
            scanner->recordID.page = 1;
            scanner->recordID.slot = 0;
        } else {
            scanner->recordID.slot += 1;
            if (scanner->recordID.slot >= totalSlots) {
                scanner->recordID.slot = 0;
                scanner->recordID.page += 1;
            }
        }

        pinPage(&recordManager->bufferPool, &scanner->bm_pageHandle, scanner->recordID.page);
        char *data = scanner->bm_pageHandle.data;

        data += (scanner->recordID.slot * size);

        initializeRecordFromData(record, data, size, &scanner->recordID);

        scanner->scanCount += 1;
        scanCount += 1;

        evalExpr(record, schema, scanner->cond, &result);

        if (result->v.boolV == TRUE) {
            unpinPage(&recordManager->bufferPool, &scanner->bm_pageHandle);
            return RC_OK;
        }
    }
}

// Main next function
extern RC next(RM_ScanHandle *scan, Record *record) {
    RC return_code = RC_RM_NO_MORE_TUPLES;
    RecordManager *recordManager = scan->rel->mgmtData;
    RecordManager *scanner = scan->mgmtData;
    Schema *schema = scan->rel->schema;

    if (!conditionExists(scan)) {
        return RC_CONDITION_NOT_FOUND;
    }

    Value *result = (Value *)malloc(sizeof(Value));

    int size = getRecordSize(schema);

    int totalSlots = PAGE_SIZE / size;
    int scanCount = scanner->scanCount;
    int attrCount = recordManager->attrCount;

    findNextTuple(scanCount, attrCount, scanner, totalSlots, size, record, schema, result);
    unpinPage(&recordManager->bufferPool, &scanner->bm_pageHandle);

    scanner->recordID.page = 1;
    scanner->recordID.slot = 0;
    scanner->scanCount = 0;

    return RC_RM_NO_MORE_TUPLES;
}


extern RC closeScan(RM_ScanHandle *scan)
{
	RC return_code = RC_OK;
	RecordManager *scanner = scan->mgmtData;
	RecordManager *recordManager = scan->rel->mgmtData;

	if (scanner->scanCount > 0 || recordManager->scanCount > 0)
	{
		scanner->recordID.page = 1;
		scanner->recordID.slot, scanner->scanCount = 0;
		unpinPage(&recordManager->bufferPool, &scanner->bm_pageHandle);
    }

	free(scan->mgmtData);
	scan->mgmtData = NULL;

	return return_code;
}


int getRecordSize(Schema *schema)
{
    int size = 0;
	int totalNumAttr = schema->numAttr;
	DataType *attr = schema->dataTypes;
	
    int i = 0;

    while (i < totalNumAttr)
    {
        switch (attr[i])
        {
            case DT_STRING:
                size = 1 + schema->typeLength[i];
                break;
            case DT_INT:
                size += sizeof(int);
                break;
            case DT_FLOAT:
                size += sizeof(float);
                break;
            case DT_BOOL:
                size += sizeof(bool);
                break;
        }

		i += 1;
    }

    return size;
}


extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema) {
        schema->attrNames = attrNames;
        schema->dataTypes = dataTypes;
        schema->keyAttrs = keys;
        schema->keySize = keySize;
        schema->numAttr = numAttr;
        schema->typeLength = typeLength;
    }
	
    return schema;
}


extern RC createRecord(Record **record, Schema *schema)
{
    RC return_code = RC_OK;

    int size = getRecordSize(schema);

    Record *newRecord = (Record *)malloc(sizeof(Record));
    newRecord->data = (char *)malloc(size);
    newRecord->id.page = -1;
    newRecord->id.slot = -1;
    newRecord->data[0] = '-';
    newRecord->data[1] = '\0';

    *record = newRecord;
    
    return return_code;
}


RC attrOffset(Schema *schema, int attrNum, int *result)
{
    RC return_code = RC_OK;
    *result = 1;
	int i = 0;

    while (i < attrNum)
    {
        int type = schema->dataTypes[i];
        *result += (type == DT_STRING) ? schema->typeLength[i] : 
                   (type == DT_INT) ? sizeof(int) : 
                   (type == DT_FLOAT) ? sizeof(float) : sizeof(bool);
		
		i += 1;
    }

    return return_code;
}


void setStringValue(Value *attr, const char* data, int strLength) {
    attr->dt = DT_STRING;
    attr->v.stringV = (char *)malloc(strLength + 1);
    if (attr->v.stringV) {
        strncpy(attr->v.stringV, data, strLength);
        attr->v.stringV[strLength] = '\0';
    }
}


void setIntValue(Value *attr, char* data) {
    int value = 0;
    memcpy(&value, data, sizeof(int));
    attr->v.intV = value;
}


void setFloatValue(Value *attr, char* data) {
    float value;
    memcpy(&value, data, sizeof(float));
    attr->v.floatV = value;
}


void setBoolValue(Value *attr, char* data) {
    bool value;
    memcpy(&value, data, sizeof(bool));
    attr->v.boolV = value;
}


Value* changeAttributeValue(int dataType, char* data, Schema *schema, int attrNum, Value *attr) {
    attr->dt = dataType;

    switch (dataType) {
        case DT_STRING:
            setStringValue(attr, data, schema->typeLength[attrNum]);
            break;
        case DT_INT:
            setIntValue(attr, data);
            break;
        case DT_FLOAT:
            setFloatValue(attr, data);
            break;
        case DT_BOOL:
            setBoolValue(attr, data);
            break;
    }

    return attr;
}


extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
	RC return_code = RC_OK;

	int offset = 0;
	Value *attr = (Value *)malloc(sizeof(Value));

	int setAttribute = attrOffset(schema, attrNum, &offset);

	char *data = record->data + offset;
	schema->dataTypes[attrNum] = attrNum == 1 ? attrNum : schema->dataTypes[attrNum];

	int dataType = schema->dataTypes[attrNum];
	attr->dt = dataType;

	changeAttributeValue(dataType, data, schema, attrNum, attr);

	*value = attr;
	return return_code;
}


size_t sizeofValue(DataType type) {
    switch (type) {
        case DT_INT:
            return sizeof(int);
        case DT_FLOAT:
            return sizeof(float);
        case DT_BOOL:
            return sizeof(bool);
        case DT_STRING:
            return sizeof(char);
        default:
            return 0;
    }
}


extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
	RC return_code = RC_OK;
    int offset = 0;

	int setAttr = attrOffset(schema, attrNum, &offset);

    if (setAttr == 0) {
        char *index = record->data + offset;
        int type = schema->dataTypes[attrNum];

        if (type == DT_STRING) {
            strncpy(index, value->v.stringV, schema->typeLength[attrNum]);
        } else {
            memcpy(index, &value->v, sizeofValue(type));
        }
    } else {
		return_code = setAttr;
		printError(return_code);

		return return_code;
	}

    return return_code;
}
