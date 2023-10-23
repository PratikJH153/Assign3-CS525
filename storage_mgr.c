#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

FILE *file;

/* Initializing the storage manager */
/*
fseek
fopen
fread
malloc
memset
fclose
free
remove
*/

//! Edge cases

// int main()
// {

//     SM_FileHandle *fhandle;

//     size_t num_blocks = PAGE_SIZE;
//     size_t block_size = sizeof(char);

//     SM_PageHandle smp = (SM_PageHandle)calloc(num_blocks, block_size);

//     initStorageManager();
//     createPageFile(fileName);
//     openPageFile(fileName, fhandle);
//     readBlock(0, fhandle, smp);

//     return 0;
// }

extern void initStorageManager(void)
{
    file = NULL;
    printf("THE STORAGE MANAGER IS INITIALIZED SUCCESSFULLY\n");
}

extern RC createPageFile(char *fileName)
{
    // check to open file if not exists:
    // malloc to allocate memory
    // memset fill the blocks with \0
    // SMFILE HANDLE file append

    RC return_code = RC_OK;

    file = fopen(fileName, "w+");
    if (file == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }
    else
    {
        size_t num_blocks = PAGE_SIZE;
        size_t block_size = sizeof(char);

        // Initializing a buffer that forms a memory block for setting the file
        SM_PageHandle smp = (SM_PageHandle)calloc(num_blocks, block_size);

        // If the buffer fails to be created then return an error of it not initialized
        if (smp == NULL)
        {
            RC_message = "MEMORY ALLOCATION CREATION FAILED";
            printError(*RC_message);
            return_code = RC_BUFFER_NOT_INIT;

            return return_code;
        }

        printf("THE FILE IS CREATED SUCCESSFULLY\n");
        // fseek(file, 0 * PAGE_SIZE, SEEK_SET);

        // Checking whether write is possible to the file and whether fwrite returning the same number of pages
        if (fwrite(smp, sizeof(char), num_blocks, file) < num_blocks)
        {
            // RC_message = "WRITE TO THE FILE WAS NOT POSSIBLE!";
            // printError(*RC_message);
            return_code = RC_WRITE_FAILED;

            return return_code;
        }
        else
        {
            RC_message = "WRITE TO THE FILE WAS POSSIBLE!\n";
            printf("%s", RC_message);
            return_code = RC_OK;
            return return_code;
        }

        // The memory is freed and the file is been closed
        free(smp);
        fclose(file);
        return return_code;
    }
}

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    RC return_code = RC_OK;
    file = fopen(fileName, "r");

    if (file == NULL)
    {
        return_code = RC_FILE_NOT_FOUND;
        RC_message = "FILE DOES NOT EXISTS!";
        printError(*RC_message);
        return return_code;
    }

    if (fHandle == NULL)
    {
        return_code = RC_FILE_HANDLE_NOT_INIT;
        RC_message = "FILE HANDLE NOT INITIALIZED!";
        printError(*RC_message);
        return return_code;
    }

    // INITIALIZE ALL THE FILE PARAMETERS TO THE SM_FILEHANDLE
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;

    // TO CALCULATE THE TOTAL NUMBER OF PAGES BY GETTING AT THE END OF THE FILE
    fseek(file, 0, SEEK_END);
    size_t totalNums = ftell(file) / PAGE_SIZE;

    fHandle->totalNumPages = totalNums;
    fHandle->mgmtInfo = file;

    // TO RESET THE POINTER AT THE START OF THE FILE
    rewind(file);
    fclose(file);

    return return_code;
}

extern RC closePageFile(SM_FileHandle *fHandle)
{
    RC return_code = RC_OK;
    file = fopen(fHandle->fileName, "r+");
    if (file == NULL)
    {
        return_code = RC_FILE_NOT_FOUND;
        RC_message = "FILE DOES NOT EXISTS!";
        printError(*RC_message);
        return return_code;
    }

    int closeFile = fclose(file);

    if (closeFile == 0)
    {
        RC_message = "FILE IS CLOSED SUCCESFULLY\n";
        printf("%s", RC_message);
        return_code = RC_OK;
    }
    else
    {
        RC_message = "PROBLEM OCCURRED WHILE CLOSING THE FILE";
        printError(*RC_message);
        return_code = RC_FILE_NOT_FOUND;
    }
    file = NULL;
    return return_code;
}

extern RC destroyPageFile(char *fileName)
{
    RC return_code;

    // First closing the file and then removing the file
    if (file != NULL)
    {
        fclose(file);
        file = NULL;
    }

    // Remove function is used to destroy the file from the directory
    int removeFile = remove(fileName);

    if (removeFile == 0)
    {
        RC_message = "FILE IS REMOVED SUCCESFULLY\n";
        printf("%s", RC_message);
        return_code = RC_OK;
    }
    else
    {
        RC_message = "PROBLEM OCCURRED WHILE DESTROYING THE FILE";
        printError(*RC_message);
        return_code = RC_FILE_NOT_FOUND;
    }

    return return_code;
}

/* Reading functions to read from the block */

extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC return_code = RC_OK;

    if (pageNum > fHandle->totalNumPages || pageNum < 0)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    file = fopen(fHandle->fileName, "r");
    if (file == NULL)
    {
        return_code = RC_FILE_NOT_FOUND;
        RC_message = "FILE DOES NOT EXISTS!";
        printError(*RC_message);

        return return_code;
    }
    else
    {
        int seek = fseek(file, pageNum * PAGE_SIZE, SEEK_SET);
        if (seek == 0)
        {
            size_t size = sizeof(char);
            size_t count = PAGE_SIZE;
            size_t elements_read = fread(memPage, size, count, file);

            printf("SIZE: %zu\n", size);
            printf("Count: %zu\n", count);
            printf("Mem Page: %s\n", memPage);
            printf("Elements Read: %zu\n", elements_read);

            if (elements_read < count)
            {
                // RC_message = "ERROR OCCURRED WHILE READING THE FILE";
                // printError(*RC_message);
                // fclose(file);

                return_code = RC_READ_NON_EXISTING_PAGE;

                return return_code;
            }
        }
        else
        {
            fclose(file);
            return_code = RC_READ_NON_EXISTING_PAGE;
            return return_code;
        }

        fHandle->curPagePos = ftell(file);
        fclose(file);
        return return_code;
    }
}

extern int getBlockPos(SM_FileHandle *fHandle)
{
    RC block_pos;
    RC return_code = RC_OK;

    if (fHandle == NULL)
    {
        return_code = RC_FILE_HANDLE_NOT_INIT;
        RC_message = "FILE HANDLE NOT INITIALIZED!\n";
        printError(*RC_message);

        return return_code;
    }

    // We are keeping the track of the current Page position while reading or writing the file
    // That helps to return the current block position
    block_pos = fHandle->curPagePos;

    return block_pos;
}

extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC return_code = RC_OK;
    int pos = 0;

    // Re-using the readBlock function but with different block position to read the block respectively
    return_code = readBlock(pos, fHandle, memPage);

    return return_code;
}

extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC return_code = RC_OK;
    int pos = fHandle->curPagePos - 1;

    // Checking whether the pos not below 0 if it is, it is a non-existing page
    if (pos < 0)
    {
        return_code = RC_READ_NON_EXISTING_PAGE;
        RC_message = "ERROR WHILE READING A NON EXISTING PAGE!";
        printError(*RC_message);

        return return_code;
    }

    // Re-using the readBlock function but with previous position to read the block respectively
    return_code = readBlock(pos, fHandle, memPage);

    return return_code;
}

extern RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC return_code = RC_OK;
    int pos = fHandle->curPagePos;

    // Re-using the readBlock function but with current position to read the block respectively
    return_code = readBlock(pos, fHandle, memPage);

    return return_code;
}

extern RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC return_code = RC_OK;
    int pos = fHandle->curPagePos + 1;

    // Checking whether the pos not exceeding the totalPages, if it is, it is an non-existing page
    if (pos >= fHandle->totalNumPages)
    {
        return_code = RC_READ_NON_EXISTING_PAGE;
        RC_message = "ERROR WHILE READING A NON EXISTING PAGE!";
        printError(*RC_message);

        return return_code;
    }

    // Re-using the readBlock function but with next position to read the block respectively
    return_code = readBlock(pos, fHandle, memPage);

    return return_code;
}

extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC return_code = RC_OK;
    int pos = fHandle->totalNumPages - 1;

    // Re-using the readBlock function but with last position position to read the block respectively
    return_code = readBlock(pos, fHandle, memPage);

    return return_code;
}

/* Writing functions to read from the block */


extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC return_code = RC_OK;

    if (pageNum > fHandle->totalNumPages || pageNum < 0)
    {
        return_code = RC_WRITE_FAILED;
        printError(return_code);
        return return_code;
    }

    file = fopen(fHandle->fileName, "r+");

    if (file == NULL)
    {
        return_code = RC_FILE_NOT_FOUND;
        printError(return_code);
        return return_code;
    }

    if (pageNum == 0)
    {
        int seek = fseek(file, pageNum * PAGE_SIZE, SEEK_SET);
        if (seek == 0)
        {
            for (int i = 0; i < PAGE_SIZE; i++)
            {
                if (feof(file))
                {
                    appendEmptyBlock(fHandle);
                }
                fputc(memPage[i], file);
            }
        }
        else
        {
            return_code = RC_WRITE_FAILED;
            printError(return_code);
            return return_code;
        }

        fHandle->curPagePos = ftell(file);
        fclose(file);
    }
    else
    {
        fHandle->curPagePos = pageNum * PAGE_SIZE;
        fclose(file);
        writeCurrentBlock(fHandle, memPage);
    }
    return RC_OK;
}

extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{

    RC return_code = RC_OK;
    file = fopen(fHandle->fileName, "r+");

    if (file == NULL)
    {
        return_code = RC_FILE_NOT_FOUND;
        printError(return_code);
        return return_code;
    }

    appendEmptyBlock(fHandle);

    fseek(file, fHandle->curPagePos, SEEK_SET);

    fwrite(memPage, sizeof(char), strlen(memPage), file);

    fHandle->curPagePos = ftell(file);

    fclose(file);
    return RC_OK;
}

extern RC appendEmptyBlock(SM_FileHandle *fHandle)
{

    RC return_code = RC_OK;

    // Initialize an empty page
    SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));

    // Move the file pointer to the end
    int seek = fseek(file, 0, SEEK_END);

    if (seek == 0)
    {
        if (emptyPage == NULL)
        {
            RC_message = "MEMORY ALLOCATION FAILED FOR EMPTY PAGE";
            printError(*RC_message);
            return_code = RC_BUFFER_NOT_INIT;
        }
        else
        {
            // memset(emptyPage, 0, PAGE_SIZE);

            // Write the empty page to the file
            size_t size = sizeof(char);
            size_t count = PAGE_SIZE;

            // Write a page of data from the emptyPage buffer to the file
            size_t elements_write = fwrite(emptyPage, size, count, file);

            if (elements_write != count)
            {
                RC_message = "ERROR OCCURRED WHILE APPENDING EMPTY PAGE";
                printError(*RC_message);
                return_code = RC_WRITE_FAILED;
            }
        }

        // Writing an empty page to the file
        fwrite(emptyPage, sizeof(char), PAGE_SIZE, file);
    }
    else
    {
        free(emptyPage);
        return_code = RC_WRITE_FAILED;
        return return_code;
    }

    free(emptyPage);

    // Increment the total number of pages in the file handle
    fHandle->totalNumPages += 1;

    return return_code;
}

/* Function to ensure that a page file has enough pages */
extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    RC return_code = RC_OK;
    if (fHandle == NULL)
    {
        return_code = RC_FILE_HANDLE_NOT_INIT;
        RC_message = "FILE HANDLE NOT INITIALIZED!";
        printError(*RC_message);

        return return_code;
    }
    else
    {
        // Getting the totalNumPages to check whether the numberOfPages does not exceed the totalNumPages in the file
        size_t totalNumPages = fHandle->totalNumPages;
        if (totalNumPages < numberOfPages)
        {
            // Add an extra empty block to the memory till the size of the file
            while (totalNumPages < numberOfPages)
            {
                appendEmptyBlock(fHandle);
            }
        }
        else
        {
            return_code = RC_WRITE_FAILED;
            // RC_message = "WRITE TO THE FILE WAS NOT POSSIBLE!";
            // ;
            // printError(*RC_message);

            return return_code;
        }
    }

    return return_code;
}
