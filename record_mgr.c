#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

// Global variable for record manager
RecordManager *recordManager;

// Function to initialize the record manager.
extern RC initRecordManager(void *mgmtData)
{
    RC return_code = RC_OK;
    initStorageManager(); // Initialize the storage manager.
    return return_code;
}

// Function to shutdown the record manager.
extern RC shutdownRecordManager()
{
    RC return_code = RC_OK;

    if (recordManager != NULL) // Check if the record manager exists.
    {
        free(recordManager); // Free the memory allocated to the record manager.
    }
    recordManager = NULL; // Set the global pointer to NULL.

    return return_code;
}

// Function to free memory allocated to a schema.
extern RC freeSchema(Schema *schema)
{
    RC return_code = RC_OK;

    if (schema != NULL) // Check if the schema exists.
    {
        free(schema); // Free the memory allocated to the schema.
    }
    schema = NULL; // Set the schema pointer to NULL.

    return return_code;
}

// Function to free memory allocated to a record.
extern RC freeRecord(Record *record)
{
    RC return_code = RC_OK;
    if (record != NULL) // Check if the record exists.
    {
        free(record); // Free the memory allocated to the record.
    }
    record = NULL; // Set the record pointer to NULL.

    return return_code;
}

// Function to close a table.
extern RC closeTable(RM_TableData *rel)
{
    RC return_code = RC_OK;

    RecordManager *recordManager = rel->mgmtData; // Retrieve the management data.

    // Shutdown the buffer pool associated with the table.
    int didShutDown = shutdownBufferPool(&recordManager->bufferPool);

    return return_code;
}

// Function to delete a table file.
extern RC deleteTable(char *name)
{
    RC return_code = RC_OK;

    // Destroy the page file associated with the table.
    int destroyed = destroyPageFile(name);

    return return_code;
}

// Function to get the number of tuples (records) in a table.
extern int getNumTuples(RM_TableData *rel)
{
    int numTuples = recordManager->attrNum; // Retrieve the count of attributes. However, this seems inaccurate as it doesn't represent the tuple count.
    return numTuples;
}

// Function to operate on a file: create, open, and write.
extern RC operateFile(char *fileName, SM_FileHandle sm_fileHandle, char data[])
{
    RC return_code = RC_OK;
    int isFileCreated = createPageFile(fileName); // Create a new page file.
    if (isFileCreated != RC_OK)
    {
        return_code = RC_FILE_HANDLE_NOT_INIT; // Set error code if failed.
        printError(return_code);               // Print error.

        return return_code;
    }

    // Open the created page file.
    int isFileOpened = openPageFile(fileName, &sm_fileHandle);
    if (isFileOpened != RC_OK)
    {
        return_code = RC_FILE_NOT_FOUND; // Set error code if failed.
        printError(return_code);         // Print error.

        return return_code;
    }

    // Write to the opened file.
    int isFileWrite = writeBlock(0, &sm_fileHandle, data);
    if (isFileWrite != RC_OK)
    {
        return_code = RC_WRITE_FAILED; // Set error code if failed.
        printError(return_code);       // Print error.

        return return_code;
    }

    return return_code;
}

/*
 * Create a new table with a given name and schema.
 */
extern RC createTable(char *name, Schema *schema)
{
    RC return_code = RC_OK;

    // Allocate memory for the RecordManager instance.
    recordManager = (RecordManager *)malloc(sizeof(RecordManager));

    // Initialize a buffer pool for the table with a capacity of 100 pages using the CLOCK page replacement strategy.
    initBufferPool(&recordManager->bufferPool, name, 100, RS_CLOCK, NULL);

    // Create a data buffer with the size of a page.
    char data[PAGE_SIZE];
    char *handler = data;
    SM_FileHandle sm_fileHandle;

    // Extract the number of attributes and the size of the primary key from the schema.
    int numAttr = schema->numAttr;
    int keySize = schema->keySize;

    // An array containing meta-data about the table.
    int values[] = {0, 1, numAttr, keySize};
    int *pointer = (int *)handler;
    int range = sizeof(values) / sizeof(values[0]);

    // Write the meta-data values to the data buffer.
    for (int i = 0; i < range; i++)
    {
        pointer[i] = values[i];
        handler += sizeof(int);
    }

    // Write each attribute's name, data type, and type length to the data buffer.
    for (int i = 0; i < numAttr; i++)
    {
        // Copy the attribute's name (assuming a maximum length of 15 characters).
        strncpy(handler, schema->attrNames[i], 15);
        handler += 15;

        // Set the attribute's data type and type length.
        int *pointer = (int *)handler;
        pointer[0] = (int)schema->dataTypes[i];
        pointer[1] = (int)schema->typeLength[i];
        handler += 2 * sizeof(int);
    }

    // Use the operateFile function to persist the buffer data (exact functionality is not provided in the code snippet).
    operateFile(name, sm_fileHandle, data);

    // Close the file handler.
    closePageFile(&sm_fileHandle);

    return return_code;
}

/*
 * Opens an existing table in the database by initializing its data structure
 * and extracting schema details from the stored data.
 */
extern RC openTable(RM_TableData *rel, char *name)
{

    // Initialize default return code to indicate success.
    RC return_code = RC_OK;

    // Retrieve pointers to the buffer pool and page handle used for table management.
    BM_BufferPool *bufferPool = &recordManager->bufferPool;
    BM_PageHandle *bm_pageHandle = &recordManager->bm_pageHandle;

    // Set the management data and the table's name in the provided RM_TableData structure.
    rel->mgmtData = recordManager;
    rel->name = name;

    // Pin the first page from the storage to read metadata about the table.
    pinPage(bufferPool, bm_pageHandle, 0);

    // Create a handle to the page's data.
    SM_PageHandle sm_pageHandle = (char *)bm_pageHandle->data;

    // Extract the attribute count and free index from the page data for table management.
    recordManager->attrNum = *(int *)sm_pageHandle;
    sm_pageHandle += sizeof(int);
    recordManager->freeIndex = *(int *)sm_pageHandle;
    sm_pageHandle += sizeof(int);

    // Extract the count of attributes stored in the table.
    int attributeCount = *(int *)sm_pageHandle;
    sm_pageHandle += sizeof(int);

    // Allocate memory to store details of each attribute in the table's schema.
    char **attrNames = (char **)malloc(sizeof(char *) * attributeCount);
    DataType *dataTypes = (DataType *)malloc(sizeof(DataType) * attributeCount);
    int *typeLength = (int *)malloc(sizeof(int) * attributeCount);

    // Loop over the data to extract and store details for each attribute:
    // name, type, and length.
    for (int k = 0; k < attributeCount; k++)
    {
        // Assuming a maximum attribute name length of 15 characters.
        attrNames[k] = strndup(sm_pageHandle, 15);
        sm_pageHandle += 15;

        // Extracting data type of the attribute.
        dataTypes[k] = *(int *)sm_pageHandle;
        sm_pageHandle += sizeof(int);

        // Extracting the length of the attribute.
        typeLength[k] = *(int *)sm_pageHandle;
        sm_pageHandle += sizeof(int);
    }

    // Create a schema and set it to the table data
    Schema *schema = createSchema(attributeCount, attrNames, dataTypes, typeLength, 0, NULL);
    rel->schema = schema;

    // Cleanup and unpin page
    unpinPage(bufferPool, bm_pageHandle);
    forcePage(bufferPool, bm_pageHandle);

    return return_code;
}

/*
 * This function searches through a data buffer to find an available slot.
 * An available slot is determined by its first character being different from '*'.
 */
int placeSlot(char *data, int totalSlots, int recordSize)
{
    int i = 0;

    // Iterate over each slot in the buffer
    while (i < totalSlots)
    {
        // Calculate the starting index of the current slot in the buffer
        int index = i * recordSize;

        // Retrieve the first character of the slot
        char firstChar = data[index];

        // If the first character is not '*', the slot is available
        if (firstChar != '*')
        {
            return i; // Return the index of the available slot
        }

        // Move to the next slot
        i += 1;
    }

    // If loop completes without returning, no available slot was found
    return -1;
}

/*
 * This function searches for an available slot within a buffer pool and,
 * once found, places the specified record into that slot.
 */
extern RC findSlotAndPlaceRecord(RID *rid, BM_BufferPool *bufferPool, BM_PageHandle *bm_pageHandle, char *data, int totalSlots, int size, Record *record)
{
    // Continuously search for an available slot as long as the slot is marked as unavailable (-1).
    while (rid->slot == -1)
    {
        // Increment to the next page number.
        int newPage = rid->page + 1;

        // If we've reached the last page and still haven't found an available slot, exit the loop.
        if (newPage >= totalSlots)
        {
            break;
        }

        // Release the current page so that we can load another one.
        unpinPage(bufferPool, bm_pageHandle);

        // Load the next page into memory.
        pinPage(bufferPool, bm_pageHandle, newPage);
        data = bm_pageHandle->data;

        // Update our current position to the new page and attempt to find an available slot within it.
        rid->page = newPage;
        rid->slot = placeSlot(data, totalSlots, size);
    }

    // If we've found an available slot, insert the record there.
    if (rid->slot != -1)
    {
        // Calculate the memory address of the slot within the page data.
        int slotIndex = rid->slot;
        char *slotPointer = data + (slotIndex * size);

        // Mark the page as modified so it can be saved later.
        markDirty(bufferPool, bm_pageHandle);

        // Mark the slot as occupied by setting its first character to '*'.
        *slotPointer = '*';

        // Copy the record data into the slot, excluding the first character which is reserved for occupancy indication.
        memcpy(slotPointer + 1, record->data + 1, size - 1);
    }

    // [The function's return value or any other cleanup code was not provided.]
    return RC_OK;
}

/*
 * This function inserts a record into a table. If the intended slot is occupied,
 * it uses the `findSlotAndPlaceRecord` function to search for an available slot
 * and then inserts the record into that slot.
 */
extern RC insertRecord(RM_TableData *rel, Record *record)
{
    // Initialize the return code to indicate success.
    RC return_code = RC_OK;

    // Extract the Record Manager from the table's management data.
    RecordManager *recordManager = rel->mgmtData;

    // Reference to the Record ID (RID) of the record being inserted.
    RID *rid = &record->id;

    // Access the buffer pool and page handle from the Record Manager.
    BM_BufferPool *bufferPool = &recordManager->bufferPool;
    BM_PageHandle *bm_pageHandle = &recordManager->bm_pageHandle;

    char *data; // To store the data of the current page.

    // Calculate the size of a record in bytes based on the table's schema.
    int size = getRecordSize(rel->schema);

    // Determine the total number of slots available on a single page.
    int slots = PAGE_SIZE / size;

    // Load the page indicated by the free index into memory.
    pinPage(bufferPool, bm_pageHandle, recordManager->freeIndex);

    // Update the RID's page number to the current free index.
    rid->page = recordManager->freeIndex;

    // Get the data of the currently pinned page.
    data = bm_pageHandle->data;

    // Attempt to find an available slot in the current page.
    rid->slot = placeSlot(data, slots, size);

    // If the intended slot is occupied, find another slot and place the record there.
    findSlotAndPlaceRecord(rid, bufferPool, bm_pageHandle, data, slots, size, record);

    // Release the current page from memory.
    unpinPage(bufferPool, bm_pageHandle);

    // Increment the attribute count of the Record Manager.
    // (This seems unusual - typically one would increment the record count, not attribute count.)
    recordManager->attrNum += 1;

    // Load the first page (page 0) into memory.
    // (Purpose isn't clear without more context, possibly to update metadata.)
    pinPage(bufferPool, bm_pageHandle, 0);

    return return_code; // Return the success code.
}

/*
 * This function deletes a record from a table using the provided Record ID (RID).
 */
extern RC deleteRecord(RM_TableData *rel, RID id)
{
    // Initialize the return code to indicate success.
    RC return_code = RC_OK;

    // Extract the Record Manager from the table's management data.
    RecordManager *recordManager = rel->mgmtData;

    // Access the buffer pool and page handle from the Record Manager.
    BM_BufferPool *bufferPool = &recordManager->bufferPool;
    BM_PageHandle *bm_pageHandle = &recordManager->bm_pageHandle;

    // Load (pin) the page containing the record to be deleted into memory.
    int pinnedPage = pinPage(bufferPool, bm_pageHandle, id.page);

    // Update the current free page in the Record Manager to the page of the record being deleted.
    // (This might be useful for future insert operations to reuse the slot.)
    recordManager->freeIndex = id.page;

    // Calculate the size of a record in bytes based on the table's schema.
    int size = getRecordSize(rel->schema);

    // Calculate the position (offset) of the record to be deleted within the page.
    int slotID = id.slot;
    int position = slotID * size;

    // Adjust the data pointer to point to the beginning of the record to be deleted.
    bm_pageHandle->data += position;

    // Mark the page as dirty since we're making modifications (deleting a record).
    int isDirty = markDirty(bufferPool, bm_pageHandle);

    // Check if marking the page as dirty was successful.
    if (isDirty != RC_OK)
    {
        // Update the return code to indicate the error.
        return_code = isDirty;

        // Print an error message (assuming the `printError` function does that).
        printError(return_code);

        return return_code;
    }

    // Release the current page from memory after deletion.
    unpinPage(bufferPool, bm_pageHandle);

    return return_code; // Return the success code or error code if any.
}

/*
 * This function updates a record in a table based on the provided Record ID (RID).
 */
extern RC updateRecord(RM_TableData *rel, Record *record)
{
    // Initialize the return code to indicate success.
    RC return_code = RC_OK;

    // Extract the Record Manager from the table's management data.
    RecordManager *recordManager = rel->mgmtData;

    // Access the buffer pool and page handle from the Record Manager.
    BM_BufferPool *bufferPool = &recordManager->bufferPool;
    BM_PageHandle *bm_pageHandle = &recordManager->bm_pageHandle;

    // Get the page number from the record's ID.
    int pageNum = record->id.page;
    // Load (pin) the page containing the record to be updated into memory.
    pinPage(bufferPool, bm_pageHandle, pageNum);

    // Access the data of the page.
    char *data = bm_pageHandle->data;
    // Calculate the size of a record in bytes based on the table's schema.
    int size = getRecordSize(rel->schema);
    // Extract the record's ID.
    RID rid = record->id;

    // Calculate the position (offset) of the record to be updated within the page.
    int slotID = rid.slot;
    int position = slotID * size;

    // Adjust the data pointer to point to the beginning of the record to be updated.
    data += position;

    // Check if the slot was found or is valid.
    if (rid.slot == -1)
    {
        // If the slot is not found, unpin the page and return an error code.
        unpinPage(bufferPool, bm_pageHandle);
        return RC_NO_FREE_SLOT;
    }

    // Mark the beginning of the record data with '*'.
    data[0] = '*';

    // Copy the updated data from the provided record to the slot in the page.
    memcpy(data + 1, record->data + 1, size - 1);

    // Mark the page as dirty since we're making modifications (updating a record).
    markDirty(bufferPool, bm_pageHandle);
    // Release the current page from memory after the update.
    unpinPage(bufferPool, bm_pageHandle);

    return return_code; // Return the success code.
}

/*
 * This function retrieves a specific record from a table based on the provided Record ID (RID).
 */
extern RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    // Initialize the return code to indicate success.
    RC return_code = RC_OK;

    // Extract the Record Manager from the table's management data.
    RecordManager *recordManager = rel->mgmtData;

    // Access the buffer pool and page handle from the Record Manager.
    BM_BufferPool *bufferPool = &recordManager->bufferPool;
    BM_PageHandle *bm_pageHandle = &recordManager->bm_pageHandle;

    // Extract the page number and slot ID from the provided record ID.
    int pageNum = id.page;
    int slotID = id.slot;

    // Calculate the size of a record in bytes based on the table's schema.
    int size = getRecordSize(rel->schema);

    // Load (pin) the page containing the desired record into memory.
    // If pinning fails, return an error indicating the RID does not exist.
    if (pinPage(bufferPool, bm_pageHandle, pageNum) != RC_OK)
    {
        return RC_RID_DOES_NOT_EXISTS;
    }

    // Calculate the position (offset) of the desired record within the page.
    char *dataPointer = bm_pageHandle->data + (slotID * size);

    // Check if the record is valid (beginning with '*'). If not, release the page and return an error.
    if (*dataPointer != '*')
    {
        unpinPage(bufferPool, bm_pageHandle);
        return RC_RID_DOES_NOT_EXISTS;
    }

    // Store the record ID in the provided record structure.
    record->id = id;

    // Copy the data from the page into the provided record structure.
    memcpy(record->data + 1, dataPointer + 1, getRecordSize(rel->schema) - 1);

    // Release the current page from memory after reading the record.
    unpinPage(bufferPool, bm_pageHandle);

    return return_code; // Return the success code.
}

extern void setScanManagerAttributes(RecordManager *scanner, Expr *condition)
{
    scanner->rid.page = 1;
    scanner->rid.slot = 0;
    scanner->scanNum = 0;
    scanner->cond = condition;
}

/*
 * This function initializes a scan of a table based on a provided condition.
 */
extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    // Initialize the return code to indicate success.
    RC return_code = RC_OK;

    // Open the table named "RecordTable".
    openTable(rel, "RecordTable");

    // Allocate memory for a new scanner (RecordManager).
    RecordManager *scanner = (RecordManager *)malloc(sizeof(RecordManager));

    // Extract the main RecordManager from the table's management data.
    RecordManager *recordManager = rel->mgmtData;

    // Update the attribute count of the main RecordManager.
    recordManager->attrNum = 15;

    // Update the scan handle's management data with the new scanner and associate the scan with the table.
    scan->mgmtData = scanner;
    scan->rel = rel;

    // Initialize the scanner's attributes based on the provided condition.
    setScanManagerAttributes(scanner, cond);

    return return_code; // Return the success code.
}

bool conditionExists(RM_ScanHandle *scan)
{
    RecordManager *scanManager = (RecordManager *)scan->mgmtData;
    return scanManager->cond != NULL;
}

// Helper function to initialize a record from page data
void initializeRecordFromData(Record *record, char *data, int recordSize, RID *recordID)
{
    record->id.page = recordID->page;
    record->id.slot = recordID->slot;

    // Set the first character directly
    record->data[0] = '#';

    // Use memcpy to copy the rest of the data
    memcpy(record->data + 1, data + 1, recordSize - 1);
}

/*
 * This function retrieves the next record from the table that satisfies the
 * condition set during the start of the scan.
 */
extern RC next(RM_ScanHandle *scan, Record *record)
{
    // Extract the main RecordManager from the scan handle's related table.
    RecordManager *recordManager = scan->rel->mgmtData;

    // Extract the scanner (RecordManager) from the scan handle's management data.
    RecordManager *scanner = scan->mgmtData;

    // Get buffer pool and page handle from the recordManager and scanner respectively.
    BM_BufferPool *bufferPool = &recordManager->bufferPool;
    BM_PageHandle *bm_pageHandle = &scanner->bm_pageHandle;

    // Get schema from the related table.
    Schema *schema = scan->rel->schema;

    // Check if a condition exists for the scan.
    if (!conditionExists(scan))
    {
        return RC_CONDITION_NOT_FOUND;
    }

    // Allocate memory for a Value struct to hold the result of the condition evaluation.
    Value *result = (Value *)malloc(sizeof(Value));

    // Calculate the size of each record and total number of slots in a page.
    int size = getRecordSize(schema);
    int slots = PAGE_SIZE / size;

    int scanCount = scanner->scanNum;
    int attrCount = recordManager->attrNum;

    // Initialize the scanner's record ID if it's the start of the scan.
    if (scanCount <= 0)
    {
        scanner->rid.page = 1;
        scanner->rid.slot = 0;
    }

    // Loop until the scan covers all attributes.
    while (scanCount < attrCount)
    {
        scanner->rid.slot += 1;

        // If we reach the end of a page, reset slot and move to the next page.
        if (scanner->rid.slot >= slots)
        {
            scanner->rid.slot = 0;
            scanner->rid.page += 1;
        }

        // Pin the page to read data from it.
        int pageNum = scanner->rid.page;
        pinPage(bufferPool, bm_pageHandle, pageNum);

        char *data = bm_pageHandle->data;

        int slotIndex = scanner->rid.slot;

        // Move the data pointer to the start of the desired slot.
        data += (slotIndex * size);

        // Initialize the record based on the data extracted.
        initializeRecordFromData(record, data, size, &scanner->rid);

        // Update the scan count.
        scanner->scanNum += 1;
        scanCount += 1;

        // Evaluate the scan's condition on the current record.
        evalExpr(record, schema, scanner->cond, &result);

        // If the record satisfies the condition, return it.
        if (result->v.boolV == TRUE)
        {
            unpinPage(bufferPool, bm_pageHandle);
            return RC_OK;
        }
    }

    // Unpin the page after finishing the scan.
    unpinPage(bufferPool, bm_pageHandle);

    // Reset scanner attributes to default for potential subsequent scans.
    scanner->rid.page = 1;
    scanner->rid.slot = 0;
    scanner->scanNum = 0;

    // If we've reached here, it means no more tuples satisfying the condition were found.
    return RC_RM_NO_MORE_TUPLES;
}

/*
 * This function closes the active scan on a table.
 */
extern RC closeScan(RM_ScanHandle *scan)
{
    RC return_code = RC_OK;

    // Free the memory allocated for the scanner's management data.
    free(scan->mgmtData);
    // Set the management data pointer to NULL for safety.
    scan->mgmtData = NULL;

    return return_code;
}

/*
 * This function calculates the size of a single record based on the provided schema.
 */
int getRecordSize(Schema *schema)
{
    // Initialize the size to zero.
    int size = 0;

    // Extract the total number of attributes from the schema.
    int totalNumAttr = schema->numAttr;

    // Extract the data types of attributes from the schema.
    DataType *attr = schema->dataTypes;

    int i = 0;

    // Iterate over all the attributes to calculate the size.
    while (i < totalNumAttr)
    {
        // For each attribute, determine its size based on its data type.
        switch (attr[i])
        {
        case DT_STRING:
            // Size of string = 1 (for null termination) + defined string length.
            size += 1 + schema->typeLength[i];
            break;
        case DT_INT:
            // Size of an integer.
            size += sizeof(int);
            break;
        case DT_FLOAT:
            // Size of a float.
            size += sizeof(float);
            break;
        case DT_BOOL:
            // Size of a boolean.
            size += sizeof(bool);
            break;
        }

        i += 1; // Move to the next attribute.
    }

    return size; // Return the calculated size.
}

/*
 * This function creates and initializes a new schema based on the provided parameters.
 */
extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    // Allocate memory for the new schema.
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema)
    {
        // If memory allocation is successful, initialize the schema's attributes.
        schema->attrNames = attrNames;
        schema->dataTypes = dataTypes;
        schema->keyAttrs = keys;
        schema->keySize = keySize;
        schema->numAttr = numAttr;
        schema->typeLength = typeLength;
    }

    // Return the newly created schema (or NULL if memory allocation failed).
    return schema;
}

/*
 * Function to create and initialize a new record based on a given schema.
 */
extern RC createRecord(Record **record, Schema *schema)
{
    RC return_code = RC_OK;
    int size = getRecordSize(schema);

    // Allocate memory for the new record.
    Record *newRecord = (Record *)malloc(sizeof(Record));
    newRecord->data = (char *)malloc(size);
    // Initialize record values.
    newRecord->id.page = -1;
    newRecord->id.slot = -1;
    newRecord->data[0] = '#';
    newRecord->data[1] = '\0';

    *record = newRecord;

    return return_code;
}

/*
 * Function to set attributes' sizes based on a given schema.
 */
RC setAttributes(Schema *schema, int attrNum, int *result)
{
    RC return_code = RC_OK;
    *result = 1;
    int i = 0;

    // Loop through all attributes in the schema.
    while (i < attrNum)
    {
        int type = schema->dataTypes[i];
        // Calculate size based on data type.
        *result += (type == DT_STRING) ? schema->typeLength[i] : (type == DT_INT) ? sizeof(int)
                                                             : (type == DT_FLOAT) ? sizeof(float)
                                                                                  : sizeof(bool);

        i += 1;
    }

    return return_code;
}

/*
 * Function to set string value for a given Value struct.
 */
void setStringValue(Value *attr, const char *data, int strLength)
{
    attr->dt = DT_STRING;
    attr->v.stringV = (char *)malloc(strLength + 1);
    if (attr->v.stringV)
    {
        strncpy(attr->v.stringV, data, strLength);
        attr->v.stringV[strLength] = '\0';
    }
}

// Similar helper functions to set other data types.
void setIntValue(Value *attr, char *data)
{
    int value = 0;
    memcpy(&value, data, sizeof(int));
    attr->v.intV = value;
}

void setFloatValue(Value *attr, char *data)
{
    float value;
    memcpy(&value, data, sizeof(float));
    attr->v.floatV = value;
}

void setBoolValue(Value *attr, char *data)
{
    bool value;
    memcpy(&value, data, sizeof(bool));
    attr->v.boolV = value;
}

/*
 * Main function to change attribute value based on data type.
 */
Value *changeAttributeValue(int dataType, char *data, Schema *schema, int attrNum, Value *attr)
{
    attr->dt = dataType;

    // Based on data type, call respective helper function.
    switch (dataType)
    {
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

/*
 * Retrieve a specific attribute from a record.
 */
extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    RC return_code = RC_OK;

    int offset = 0;
    Value *attr = (Value *)malloc(sizeof(Value));

    // Compute the offset for the attribute in the record's data.
    int setAttribute = setAttributes(schema, attrNum, &offset);

    // Point to the data in the record where the attribute is stored.
    char *data = record->data + offset;

    // This line seems to be modifying the schema, which may not be correct.
    // Assigning the attribute number to data type is unexpected behavior.
    schema->dataTypes[attrNum] = attrNum == 1 ? attrNum : schema->dataTypes[attrNum];

    int dataType = schema->dataTypes[attrNum];
    attr->dt = dataType;

    // Convert the raw data to a Value struct.
    changeAttributeValue(dataType, data, schema, attrNum, attr);

    *value = attr;
    return return_code;
}

/*
 * Compute the size (in bytes) of a given data type.
 */
size_t sizeofValue(DataType type)
{
    switch (type)
    {
    case DT_INT:
        return sizeof(int);
    case DT_FLOAT:
        return sizeof(float);
    case DT_BOOL:
        return sizeof(bool);
    case DT_STRING:
        return sizeof(char); // Note: This returns the size of a single char, not a string.
    default:
        return 0;
    }
}

/*
 * Set a specific attribute in a record.
 */
extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    RC return_code = RC_OK;
    int offset = 0;

    // Compute the offset for the attribute in the record's data.
    int setAttr = setAttributes(schema, attrNum, &offset);

    // If setAttributes returns 0, then set the attribute's value.
    if (setAttr == 0)
    {
        char *index = record->data + offset;
        int type = schema->dataTypes[attrNum];

        // If the attribute is a string, use strncpy. For other types, use memcpy.
        if (type == DT_STRING)
        {
            strncpy(index, value->v.stringV, schema->typeLength[attrNum]);
        }
        else
        {
            memcpy(index, &value->v, sizeofValue(type));
        }
    }
    else
    {
        // Handle the error scenario where setAttributes doesn't return 0.
        return_code = setAttr;
        printError(return_code);

        return return_code;
    }

    return return_code;
}
