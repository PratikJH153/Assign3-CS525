**Github Link**
https://github.com/PratikJH153/Assign3-CS525

**Running the script**
- Step 1- $ make clean
- Step 2- $ make 
- Step 3- $ ./test_assign1

**Contributions**
Priyanka Karan: openTable(RM_TableData *rel, char *name), getRecord(RM_TableData *rel, RID id, Record *record), findFreeSlot(char *data, int totalSlots, int recordSize), findFreeSlot(char *data, int totalSlots, int recordSize), shutdownRecordManager(), setFloatValue(Value attr, char data), setBoolValue(Value attr, char data)


Shikha Sharma: operateFile(char *fileName, SM_FileHandle sm_fileHandle, char data[]), setAttr(Record *record, Schema *schema, int attrNum, Value *value), startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond), closeScan(RM_ScanHandle *scan), freeSchema(Schema *schema), setStringValue(Value attr, char data, Schema *schema, int attrNum), 


Pratik Jadhav: insertRecord(RM_TableData *rel, Record *record), next(RM_ScanHandle *scan, Record *record), createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys), attrOffset(Schema *schema, int attrNum, int *result), freeRecord(Record *record), conditionExists(RM_ScanHandle *scan) 


Vedashree Shinde: deleteRecord(RM_TableData *rel, RID id), updateRecord(RM_TableData *rel, Record *record), sizeofValue(DataType type), changeAttributeValue(int dataType, char* data, Schema *schema, int attrNum, Value *attr), closeTable(RM_TableData *rel), initRecordManager(void *mgmtData), setIntValue(Value attr, char data)


Vedant Landge: createTable(char *name, Schema *schema), getAttr(Record *record, Schema *schema, int attrNum, Value **value), getRecordSize(Schema *schema), createRecord(Record **record, Schema *schema), getNumTuples(RM_TableData *rel), initializeRecordFromData(Record *record, char *data, int recordSize, RID *recordID), 

**Description**

**initRecordManager(void *mgmtData)**
- `initRecordManager` is an initialization function that prepares the system's storage manager component for future operations. 
- This function sets the stage for other database operations like creating tables, inserting records, and more.

**shutdownRecordManager()**
- `shutdownRecordManager()` ensures a graceful termination of the record manager component in the database system.
- It checks if the global `recordManager` pointer is not `NULL` and then releases the memory allocated to it using `free()`.
- After freeing the memory, recordManager is set to `NULL` to indicate it's no longer in use.
- The function returns a status indicator, typically `RC_OK`, signaling a successful shutdown operation.

**freeSchema(Schema *schema)**
- `freeSchema(Schema *schema)` is a function designed to release the memory associated with a given `Schema` structure in the database system.
- The function first checks if the provided schema pointer is not `NULL`.
- If the `schema` is valid, the memory occupied by it is released using the `free()` function.
- After releasing the memory, the `schema` pointer is set to `NULL` to prevent any dangling pointer issues.
- The function then returns a status code, typically `RC_OK`, to indicate that the operation was successful.

**freeRecord(Record *record)**
- `freeRecord(Record *record)` is a function used to release the memory associated with a given `Record` structure in the database system.
- It starts by verifying if the provided `record` pointer isn't `NULL`.
- Upon validation, the memory occupied by the record is freed using the `free()` function.
- Subsequently, the `record` pointer is set to `NULL` to avoid potential dangling pointer situations.
- Finally, the function returns a status code, typically `RC_OK`, signifying a successful execution.

**closeTable(RM_TableData *rel)**
- `closeTable(RM_TableData *rel)` is a function intended to close an open table.
- Inside the function, the associated `RecordManager` instance is retrieved from the `rel->mgmtData`.
- Subsequently, the buffer pool associated with the table, maintained by the `RecordManager`, is shut down using the `shutdownBufferPool(&recordManager->bufferPool)` function.
- Finally, the function returns a status code, typically `RC_OK`, to indicate successful completion.

**deleteTable(char *name)**
- `deleteTable(char *name)` is a function used to delete a table with a given name.
The table deletion is achieved by calling `destroyPageFile(name)`, which removes the page file associated with the table's name.
The function then returns a status code, which is usually `RC_OK`, indicating successful execution.

**getNumTuples(RM_TableData *rel)**
- `getNumTuples(RM_TableData *rel)` is a function designed to retrieve the number of tuples present in a table.
It achieves this by accessing the `tuplesCount` field of the global `recordManager`.
The function then returns the count of tuples as an integer.

**operateFile(char *fileName, SM_FileHandle sm_fileHandle, char data[])**
- `operateFile(char *fileName, SM_FileHandle sm_fileHandle, char data[])` is a function designed to create, open, and write data to a file.
- Firstly, it tries to create a file with the given `fileName` using `createPageFile(fileName)`. If the creation fails, it returns an error code `RC_FILE_HANDLE_NOT_INIT`.
- After successfully creating the file, it attempts to open the file using `openPageFile(fileName, &sm_fileHandle)`. If the file cannot be opened, it returns an error code `RC_FILE_NOT_FOUND`.
- Once the file is open, it writes the provided `data` to the first block of the file using `writeBlock(0, &sm_fileHandle, data)`. If the write operation fails, it returns an error code `RC_WRITE_FAILED`.
- If all operations succeed, the function returns `RC_OK`, indicating success.

**createTable(char *name, Schema *schema)**
- `createTable(char *name, Schema *schema)` initializes a `recordManager` using the `malloc` function, setting aside memory for the `RecordManager` type. 
- It then initializes a buffer pool for this table and constructs a data array containing schema details. 
- Strings like attribute names from the schema are copied to this data array using the `strncpy` function. 
- The populated data array is then written to a file using the `operateFile` function.

**openTable(RM_TableData *rel, char *name)**
- `openTable(RM_TableData *rel, char *name)`: Opens the table represented by the given name.
- The management data and name of the table are set at the beginning of the function.
- The function reads the first page of the table's file to extract the schema details and other metadata. This is achieved using `pinPage` to load the page into memory.
- After reading the tuple count and free page details, the function reads the number of attributes `(attributeCount)`.
- It then initializes arrays (`attrNames`, `dataTypes`, and `typeLength`) using `malloc` to store the attribute details.
- Through a loop, the function extracts the attribute names, data types, and type lengths, storing them in the initialized arrays. The `strndup` function is used to duplicate a specified portion of each attribute name string.
- Once all attribute details are loaded into memory, the function uses `createSchema` to generate a Schema object. This `schema` is then attached to the table data. (createSchema constructs a Schema structure using the provided attributes and their details. This schema represents the structure of the table.)
- The function concludes by unpinning the page `unpinPage` and forcing it back `forcePage` to disk (if necessary) to ensure that any changes are persisted.

**findFreeSlot(char *data, int totalSlots, int recordSize)**
- `findFreeSlot(char *data, int totalSlots, int recordSize)`: This function checks a page's data to determine the first free slot that can accommodate a new record.
- It takes in three arguments:
    `data`: A pointer to the start of the page's data.
    `totalSlots`: The total number of slots present in the page. This is calculated based on the page size and record size.
    `recordSize`: The size (in bytes) of each record within the page.
- The function works by iterating over each slot in the page's data (up to `totalSlots`). For each slot, it checks the first character of the slot. If this character is not '+', the slot is considered free.
- If a free slot is found, the function returns the slot number (i.e., the index of the slot). If no free slot is found, it returns `-1`.
- The character '+' seems to be a marker or flag indicating that a slot is occupied by a valid record. Conversely, any other character in the first position of a slot would imply that the slot is available.

**insertRecord(RM_TableData *rel, Record *record)**
- `insertRecord(RM_TableData *rel, Record *record)`: This function inserts a new record into a table.
- Arguments:
    `rel`: A pointer to the table's meta-data, which contains important details like schema, management data, and table name.
    `record`: A pointer to the record that needs to be inserted.
- The function starts by determining the size of a record (`recordSize`) based on the table's schema.
- The total number of slots (`totalSlots`) available in a page is calculated by dividing the page size with the record size.
- It pins the page where a new record can potentially be added. The free page number is retrieved from the table's management data (`recordManager->freePage`).
- Using the findFreeSlot function, it identifies a free slot in the pinned page where the record can be inserted.
- If no free slot is found in the current page, the function unpins the page `unpinPage`, increments the page number, and pins the next page `pinPage`. This process is repeated until a free slot is found.
- Once a free slot is found, the function prepares to write the new record into this slot.
- The page is marked as dirty `markDirty` because its content will be modified.
- The first character in the slot is set to '+', indicating that the slot will now be occupied by a valid record in `memcpy`.
- The record's data is then copied into the slot, after which the page is unpinned.
- Finally, the total count of records (tuples) in the table is incremented.

**deleteRecord(RM_TableData *rel, RID id)**
- `deleteRecord(RM_TableData *rel, RID id)`: This function deletes a record from a table based on the provided `RID`.
- Arguments:
    `rel`: A pointer to the table's meta-data, which includes crucial details such as schema, management data, and table name.
    `id`: The `RID` which represents the page and slot number of the record that needs to be deleted.
- It starts by pinning the page that contains the record that needs to be deleted.
- The function then updates the `freePage` attribute in the `RecordManager` to reflect the page number of the record that will be deleted. This helps in indicating that there might be a free slot available in this page for future record insertions.
- Using the schema's details, it calculates the size of a record.
With the record size and slot number (`id.slot`), the exact position of the record within the page is determined.
- The page is marked as dirty `markDirty` since its content will be modified.
- The record's slot is marked as available (this can be done by updating a specific flag/character in the slot to indicate its emptiness, although this step seems to be missing in the provided code).
- The page is then unpinned `unpinPage`, which means it can now be written back to the disk if necessary.
- In summary, the function ensures that the specified record is marked as deleted, and the corresponding slot in the page becomes available for future use.

**updateRecord(RM_TableData *rel, Record *record)**
- `updateRecord(RM_TableData *rel, Record *record)`: The function aims to update an existing record within a table based on the `RID` found in the `record` structure.
- Arguments:
    `rel`: A pointer to the table's meta-data, which contains details such as schema, management data, and table name.
    `record`: A pointer to the record containing the new data and the `RID` (Record Identifier) specifying where the record resides.
- The process begins by pinning the page that contains the record to be updated. Pinning ensures that the page remains in memory during the update process.
- With the page pinned, a pointer (`data`) to the start of the page's data is obtained.
- The size of a record is computed based on the table's schema.
- Using the slot number (`id.slot`) from the record's `RID`, the function calculates the exact position of the record that needs to be updated within the page.
- The record's initial byte is set to '+', possibly indicating that the record is valid or active.
- The `memcpy` function is then used to copy the new data (from the input `record`) to the specified position in the page, excluding the initial validity byte.
- Once the data is copied, the page is marked as dirty `markDirty`. This indicates that its content has changed and needs to be written back to the disk at some point.
- Finally, the page is unpinned `unpinPage`, signaling that the update operation is complete and the page can be released from memory if needed.

**getRecord(RM_TableData *rel, RID id, Record *record)**
- `getRecord(RM_TableData *rel, RID id, Record *record)`: This function fetches a record identified by its `RID` from a table and populates the provided record structure with its data.
- Arguments:
    `rel`: A pointer to the table's meta-data, which contains details such as schema, management data, and table name.
    `id`: A Record Identifier (RID) structure containing the page and slot number of the record to retrieve.
    `record`: A pointer to the record structure that will store the retrieved data.
- The function begins by extracting the table's management data (`recordManager`) and initializing the `bufferPool` and `pageHandle` pointers.
- It then attempts to pin the page containing the desired record. If unsuccessful, the function returns an error indicating the `RID` does not exist.
- If the page is pinned successfully, the function calculates the exact position of the desired record within the page using the slot number (`id.slot`) from the `RID` and the size of a record (based on the table's schema).
- Before extracting the record's data, the function checks the first byte of the record (at the computed position). If the byte is not '+', this indicates the record is invalid or does not exist, and the function returns an error after unpinning the page.
- If the record is valid, the `RID` is set to the record structure, and the record's data (excluding the initial validity byte) is copied to the `record` structure using the `memcpy` function.
- Finally, the page is unpinned, and the function returns a success code.

**startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)**
- `startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)`: This function prepares a scan over the table to retrieve records that match a given condition.
Arguments:
    `rel`: A pointer to the table's meta-data, which contains details such as schema, management data, and table name.
    `scan`: A pointer to a structure that manages the scanning process, including storing the position of the current scan and other relevant details.
    `cond`: A pointer to the condition/expression based on which records will be filtered during the scan.
- The function first opens the table named "RecordTable" for scanning.
Next, it allocates memory for a new `RecordManager` instance, which will be used to manage the scanning process.
- The existing `recordManager` from the table meta-data (`rel`) is fetched. For demonstration purposes, the total number of tuples (`tuplesCount`) is set to 15. This value may vary depending on the actual usage.
- The `scan` structure's management data (`mgmtData`) is initialized with the newly created `scanner`. This `scanner` will keep track of the scan's progress and other necessary details.
    - The page of the `scanner's recordID` is set to 1, indicating that scanning will start from the second page (assuming the first page is reserved for metadata or other purposes).
    - The slot of the `scanner's recordID` is set to 0, indicating that scanning will start from the first slot of the specified page.
    - The `scanCount` is set to 0, which could keep track of the number of records scanned.
    - The provided condition (`cond`) is associated with the `scanner`, which will be used to filter records during the scan.
- The `scan` structure's relation pointer (`rel`) is set to point to the input table's meta-data (`rel`), linking the scan process with the respective table.
- Finally, the function returns a success code.

**conditionExists(RM_ScanHandle *scan)**
- `RecordManager *scanManager = (RecordManager *)scan->mgmtData`: This extracts the management data of the scanning process, which contains details about the current scan, and assigns it to `scanManager`.
- `return scanManager->condition != NULL`: This checks if the condition in `scanManager` is not `NULL`. If a condition is present (not `NULL`), it returns `true`, indicating that a condition exists for the scan. Otherwise, it returns `false`.

**initializeRecordFromData(Record *record, char *data, int recordSize, RID *recordID)**
- `record->id.page = recordID->page`; and `record->id.slot = recordID->slot`;: This sets the page and slot details for the record from the provided `recordID`.
- `char *dataPointer = record->data`;: This assigns the record's data address to `dataPointer`.
- `*dataPointer = '-'`;: This sets the first character of the record's data to '-'.
- `memcpy(++dataPointer, data + 1, recordSize - 1)`;: This copies the data to the record, starting from the second byte of the source data (`data + 1`) to one byte less than the provided record size (`recordSize - 1`). The destination begins from the second byte of the record's data (`++dataPointer`) due to the '-' assigned earlier.

**next(RM_ScanHandle *scan, Record *record)**
- `next(RM_ScanHandle *scan, Record *record)`: The function begins by extracting scan and table management data.
- It then checks if a scanning condition exists using `conditionExists(scan)`. If not, it returns `RC_CONDITION_NOT_FOUND`.
- Several counters and variables are initialized for tracking the records.
- A loop is started which will iterate over the records. The iteration continues until the `scanCount` is less than `tuplesCount`.
- For each iteration, the function adjusts the record ID based on the current `scanCount` to point to the next record.
- The current page, where the record resides, is fetched using the `pinPage` function.
- The data pointer is then adjusted to point to the specific slot containing the record.
- Using the `initializeRecordFromData` function, the record is initialized.
- The `evalExpr` function is used to evaluate if the current record satisfies the scanning condition.
- If a match is found (i.e., if the condition is satisfied), the function unpins the page using `unpinPage` and returns `RC_OK`.
- If no match is found, the loop continues to the next record.
- Once all records are checked, the function resets the `scanCount` and record ID for future scans.

**closeScan(RM_ScanHandle *scan)**
- `closeScan(RM_ScanHandle *scan)`: The function begins by initializing a return code `return_code` to `RC_OK` (which typically stands for a successful operation).
- It extracts the scan management data (`scanner`) and the table management data (`recordManager`) from the provided scan handle.
- A check is performed to see if there are any records that have been scanned (`scanner->scanCount > 0`). If so:
    - The function unpins any pages associated with the active scan using the `unpinPage` function. This step ensures that any pinned pages (due to the scanning operation) are released, freeing up buffer pool space.
    - The scan counter `scanCount` is reset to 0.
    - The record ID (`recordID`) is reset to its initial value (i.e., the first slot of the first page).
- The memory occupied by the scan management data (`scan->mgmtData`) is freed up.
- The `scan->mgmtData` pointer is set to `NULL` to ensure that there are no dangling pointers.

**getRecordSize(Schema *schema)**
- `getRecordSize(Schema *schema)`: The function initializes the `size` variable to 0, which will hold the total size of the record.
- It also initializes the loop control variable `i` to 0.
-  `totalNumAttr` is assigned the total number of attributes present in the schema.
- `attr` is assigned the data types of the attributes.
- A while loop runs for each attribute in the schema.
    - Inside the loop, the function checks the data type of the current attribute using a switch-case.
        - For `DT_STRING`: The size is incremented by 1 plus the length of the string (defined in schema->typeLength[i]).
        - For `DT_INT`: The size is incremented by the size of an integer.
        - For `DT_FLOAT`: The size is incremented by the size of a float.
        - For `DT_BOOL`: The size is incremented by the size of a boolean.
    - The loop control variable `i` is incremented to move to the next attribute.
- After iterating through all attributes, the function returns the computed `size`.

**Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)**
- `Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)`: The function starts by allocating memory for a new `Schema` structure.
- If the allocation is successful (i.e., `schema` is not `NULL`), it populates the `Schema` fields with the provided input values:
    - `numAttr` specifies the number of attributes in the schema.
    - `attrNames` points to the array of attribute names.
    - `dataTypes` points to the array of attribute data types.
    - `typeLength` points to the array of attribute lengths (useful for string data types).
    - `keySize` specifies the number of attributes that are part of the primary key.
    - `keyAttrs` points to the array of indices indicating which attributes are primary keys.
- Finally, the function returns the initialized `schema`.

**createRecord(Record **record, Schema *schema)**
- `createRecord(Record **record, Schema *schema)`: The function starts by determining the size required for a record based on the provided schema using the `getRecordSize` function.
- It then allocates memory for a new `Record` structure.
- Next, memory is allocated for the data of the record based on the computed size.
- The ID (page and slot) of the record is initialized to `-1`, indicating that it hasn't been assigned yet.
- The first character of the `data` field is initialized to `'-'`, though the significance of this might depend on the surrounding context (e.g., it could indicate a "free" or "unused" slot in some systems).
- The second character of the `data` field is initialized to `'\0'`, a null-terminator, which might be used in string processing or as an end marker.
- Finally, the pointer `record` is updated to point to the newly created `Record` structure.

**attrOffset(Schema *schema, int attrNum, int *result)**
- `attrOffset(Schema *schema, int attrNum, int *result)`: The function initializes the result to `1`. This might indicate a reserved byte (perhaps for a record marker or another metadata indicator).
- The function then iterates through each attribute in the schema until it reaches the attribute specified by `attrNum`.
- For each attribute it encounters, the function increments the result based on the data type of the attribute:
    - If the attribute is of type `DT_STRING`, the `result` is incremented by the length specified in the `schema->typeLength` for that attribute.
    - If the attribute is of type `DT_INT`, the `result` is incremented by the size of an integer.
    - If the attribute is of type `DT_FLOAT`, the `result` is incremented by the size of a float.
    - If the attribute is of any other type (assuming `DT_BOOL`), the `result` is incremented by the size of a boolean.
- After processing each attribute up to `attrNum`, the function returns the return_code indicating successful execution.

**setStringValue(Value *attr, char* data, Schema *schema, int attrNum)**
- `setStringValue(Value *attr, char* data, Schema *schema, int attrNum)`: The function retrieves the length of the string for the specified attribute (`attrNum`) from the schema.
- It then allocates memory for the string, taking into account an extra byte for the null-terminator (`\0`).
- Using the `strncpy` function, it extracts the string value from the `data` block and stores it in the `Value` structure (`attr->v.stringV`).
- The function ensures that the extracted string is null-terminated by manually setting the last byte to `\0`.

**setIntValue(Value *attr, char* data)**
- `setIntValue(Value *attr, char* data)`: The function initializes a local integer variable value to 0.
- Using the `memcpy` function, it extracts the integer value from the `data` block and stores it in the local `value` variable.
- It then assigns this extracted integer value to the `Value` structure (`attr->v.intV`).

**setFloatValue(Value *attr, char* data)**
- `setFloatValue(Value *attr, char* data)`: The function starts by initializing a local float variable `value`.
- With the `memcpy` function, it extracts the float value from the `data` buffer, placing it in the local `value` variable.
- Following this, the extracted float value is assigned to the Value structure (`attr->v.floatV`).

**setBoolValue(Value *attr, char* data)**
- `setBoolValue(Value *attr, char* data)`: The function starts by defining a local boolean variable named `value`.
- The `memcpy` function is then employed to pull the boolean `value` from the data buffer, and it is placed into the `value` variable.
- Subsequently, the extracted boolean value is stored in the `ValuE` structure, specifically in the `attr->v.boolV` field.

**changeAttributeValue(int dataType, char* data, Schema *schema, int attrNum, Value *attr)**
- `changeAttributeValue(int dataType, char* data, Schema *schema, int attrNum, Value *attr)`: The function starts by setting the data type of the `Value` structure to the provided `dataType`.
- A switch-case structure is then used to handle the extraction and setting of the value based on its type:
    - If the attribute is a string (`DT_STRING`), the `setStringValue` function is called to extract and set the string value from the `data` buffer.
    - If the attribute is an integer (`DT_INT`), the `setIntValue` function is called.
    - If the attribute is a float (`DT_FLOAT`), the `setFloatValue` function is called.
    - If the attribute is a boolean (`DT_BOOL`), the `setBoolValue` function is called.
-  After the appropriate value is set, the updated `Value` structure pointer is returned.

**getAttr(Record *record, Schema *schema, int attrNum, Value **value)**
- `getAttr(Record *record, Schema *schema, int attrNum, Value **value)`: The function starts by computing the offset (the position in the serialized data) of the desired attribute using the `attrOffset` function.
- It allocates memory for the `Value` structure that will store the attribute's value.
Using the computed offset, the function points to the start of the attribute's data in the serialized record.
- The line `schema->dataTypes[attrNum] = attrNum == 1 ? attrNum : schema->dataTypes[attrNum]`; seems odd. - It conditionally alters the schema's data type based on the attribute number. This can potentially introduce bugs and inconsistencies, and it's unclear why this operation is needed without additional context.
- It extracts the data type of the desired attribute from the schema.
- The `changeAttributeValue` function is then called to update the `Value` structure with the actual value of the attribute from the serialized data based on its type.
- Finally, the populated `Value` structure pointer is set to the provided `value` double pointer.

**sizeofValue(DataType type)**
- The function uses a `switch` statement to determine the size of the input data type.
- For `DT_INT`, it returns the size of an integer.
- For `DT_FLOAT`, it returns the size of a float.
- For `DT_BOOL`, it returns the size of a boolean.
- For `DT_STRING`, it returns the size of a char. Note that this will return the size of a single character, not the size of a full string. If you want to determine the size of a string with a known length, you would typically multiply the result of this case by the string length.
- If the input data type doesn't match any of the predefined data types, the function returns 0.

**setAttr(Record *record, Schema *schema, int attrNum, Value *value)**
- Initially, the function sets the result code to `RC_OK`.
- The function calls `attrOffset` to determine the byte offset of the given attribute in the record's data.
- If `attrOffset` returns 0 (indicating success), the function determines where within the `record->data` the attribute should be placed using the calculated `offset`.
- The function checks the data type of the attribute (using `schema->dataTypes[attrNum]`).
- If the data type is a string (`DT_STRING`), it copies the string from the `value` to the record using `strncpy`. The length of the string is based on the `schema->typeLength[attrNum]`.
- If the data type is not a string, the function uses `memcpy` to copy the value from the `value` structure to the record. The number of bytes copied is determined by the `sizeofValue` function.
- If `attrOffset` did not return 0 (indicating an error), the function sets the `return_code` to the error code returned by `attrOffset`, prints an error message using `printError`, and returns the error code.