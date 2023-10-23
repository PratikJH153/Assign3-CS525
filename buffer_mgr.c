#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "replacement_mgr_strat.h"
#include <math.h>

/*
Task is to maintain an array which is called as Buffer Pool to contain the pages as a cache in the buffer pool
which is fetched from the disk and stored in the memory
Page Table: Is a hashmap which keeps track of pages that currently in the memory
*/

//  int main()
// {
//     return 0;
//  }

// Returns the page file by the position of the page
// A buffer pool consists of a fixed number of pages called as frames to store the pages from the disk
// Buffer Manager checks whether the page is already in cache (Buffer pool) (If condition)

/*
If the page already exists in the buffer pool then it returns the pointer for the page
If not, then the page is loaded to the appropriate frame in the buffer pool which is done by the replacement strategy
Once, the frame is found, the page is loaded and the pointer is returned to the user
*/

// Replacement strategies
// LRU
// FIFO
// Clock
// LFU
// LRU-k

int maxBufferSize = 0;
int noOfPagesRead = 0;
int noOfPagesWrite = 0;
int hit = 0;
int clockPointer = 0;

extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
						 const int numPages, ReplacementStrategy strategy,
						 void *stratData)
{
	RC return_code = RC_OK;
	if (bm != NULL)
	{
		bm->numPages = numPages;
		bm->strategy = strategy;
		bm->pageFile = (char *)pageFileName;
		maxBufferSize = numPages;
	}

	Frame *frame = malloc(numPages * sizeof(Frame));
	int i = 0;

	while (i < maxBufferSize)
	{
		frame[i].bm_PageHandle.data = NULL;
		frame[i].bm_PageHandle.pageNum = -1;
		frame[i].dirtyCount = 0;
		frame[i].fixCount = 0;
		frame[i].hit = 0;

		i += 1;
	}

	bm->mgmtData = frame;

	return return_code;
}

extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	RC return_code = RC_OK;
	Frame *frame = (Frame *)bm->mgmtData;
	int didPagesWrite = forceFlushPool(bm);

	if (didPagesWrite == RC_OK)
	{
		int i = 0;
		while (i < maxBufferSize)
		{
			if (frame[i].fixCount != 0)
			{
				return RC_READ_NON_EXISTING_PAGE;
			}
			i += 1;
		}
		free(frame);
	}
	else
	{
		return_code = RC_WRITE_FAILED;
		printError(return_code);
	}

	return return_code;
}

extern RC forceFlushPool(BM_BufferPool *const bm)
{
	RC return_code = RC_OK;
	Frame *frame = (Frame *)bm->mgmtData;

	int i = 0;
	while (i < maxBufferSize)
	{
		if (frame[i].dirtyCount == 1 && frame[i].fixCount == 0)
		{
			SM_FileHandle sm_fileHandle;
			openPageFile(bm->pageFile, &sm_fileHandle);

			int didWrite = writeBlock(frame[i].bm_PageHandle.pageNum, &sm_fileHandle, frame[i].bm_PageHandle.data);

			if (didWrite == RC_OK)
			{
				frame[i].dirtyCount = 0;
				noOfPagesWrite += 1;
			}
			else
			{
				return_code = RC_WRITE_FAILED;
				printError(return_code);

				return return_code;
			}
		}

		i += 1;
	}

	return return_code;
}

extern RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	RC return_code = RC_OK;
	Frame *frame = (Frame *)bm->mgmtData;

	int i = 0;

	while (i < maxBufferSize)
	{
		if (frame[i].bm_PageHandle.pageNum == page->pageNum)
		{
			frame[i].dirtyCount = 1;
			break;
		}

		i += 1;
	}

	return return_code;
}

extern RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	RC return_code = RC_OK;
	Frame *frame = (Frame *)bm->mgmtData;

	int i = 0;

	while (i < maxBufferSize)
	{
		if (frame[i].bm_PageHandle.pageNum == page->pageNum)
		{
			frame[i].fixCount -= 1;
			break;
		}

		i += 1;
	}

	return return_code;
}

extern RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	RC return_code = RC_OK;
	Frame *frame = (Frame *)bm->mgmtData;

	int i = 0;
	while (i < maxBufferSize)
	{
		if (frame[i].bm_PageHandle.pageNum == page->pageNum)
		{
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			int didWrite = writeBlock(frame[i].bm_PageHandle.pageNum, &fh, frame[i].bm_PageHandle.data);

			if (didWrite == RC_OK)
			{
				frame[i].dirtyCount = 0;
				noOfPagesWrite += 1;
			}
			else
			{
				return_code = RC_WRITE_FAILED;
				printError(return_code);

				return return_code;
			}
		}

		i += 1;
	}

	return return_code;
}

extern RC firstPinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
					   const PageNumber pageNum, Frame *frame)
{
	RC return_code = RC_OK;
	SM_FileHandle fh;

	int didOpen = openPageFile(bm->pageFile, &fh);
	frame[0].bm_PageHandle.data = (SM_PageHandle)malloc(PAGE_SIZE);

	int didRead = readBlock(pageNum, &fh, frame[0].bm_PageHandle.data);

	if (didRead == RC_OK)
	{
		frame[0].bm_PageHandle.pageNum = pageNum;
		frame[0].fixCount += 1;
	}
	else
	{
		return_code = RC_READ_NON_EXISTING_PAGE;
		printError(return_code);

		return return_code;
	}

	noOfPagesRead = 0;
	hit = 0;

	frame[0].hit = hit;
	page->pageNum = pageNum;
	page->data = frame[0].bm_PageHandle.data;

	return return_code;
}

extern RC fullBufferPinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
				  const PageNumber pageNum)
{
	RC return_code = RC_OK;
	Frame *newPage = (Frame *)malloc(sizeof(Frame));

	SM_FileHandle fh;
	int didOpen = openPageFile(bm->pageFile, &fh);

	newPage->bm_PageHandle.data = (SM_PageHandle)malloc(PAGE_SIZE);

	int didRead = readBlock(pageNum, &fh, newPage->bm_PageHandle.data);

	if (didRead == RC_OK){
		newPage->bm_PageHandle.pageNum = pageNum;
		newPage->dirtyCount = 0;
		newPage->fixCount = 1;

		noOfPagesRead += 1;
		hit += 1;

		newPage->hit = bm->strategy == RS_LRU ? hit : 1;
		page->pageNum = pageNum;
		page->data = newPage->bm_PageHandle.data;
	} else {
		return_code = RC_READ_NON_EXISTING_PAGE;
		printError(return_code);

		return return_code;
	}

	if (bm->strategy == RS_FIFO)
	{
		// Using FIFO algorithm
		FIFO(bm, newPage, noOfPagesRead, noOfPagesWrite, maxBufferSize);
	}
	else if (bm->strategy == RS_LRU)
	{
		// Using LRU algorithm
		LRU(bm, newPage, maxBufferSize, noOfPagesWrite);
	}
	else if (bm->strategy == RS_CLOCK)
	{
		// Using CLOCK algorithm
		CLOCK(bm, newPage, clockPointer, maxBufferSize, noOfPagesWrite);
	}

	return return_code;
}

extern RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
				  const PageNumber pageNum)
{
	RC return_code = RC_OK;
	Frame *frame = (Frame *)bm->mgmtData;

	if (frame[0].bm_PageHandle.pageNum == -1)
	{
		int pin = firstPinPage(bm, page, pageNum, frame);

		return pin;
	}
	else
	{
		bool bufferFull = true;
		int i = 0;
		while (i < maxBufferSize)
		{
			if (frame[i].bm_PageHandle.pageNum != -1)
			{
				if (frame[i].bm_PageHandle.pageNum == pageNum)
				{
					frame[i].fixCount++;
					bufferFull = false;
					hit += 1;

					frame[i].hit = bm->strategy == RS_LRU ? hit : 1;
					page->pageNum = pageNum;
					page->data = frame[i].bm_PageHandle.data;

					clockPointer += 1;
					break;
				}
			}
			else
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				frame[i].bm_PageHandle.data = (SM_PageHandle)malloc(PAGE_SIZE);

				readBlock(pageNum, &fh, frame[i].bm_PageHandle.data);
				frame[i].bm_PageHandle.pageNum = pageNum;
				frame[i].fixCount = 1;

				noOfPagesRead += 1;
				hit += 1;

				frame[i].hit = bm->strategy == RS_LRU ? hit : 1;
				page->pageNum = pageNum;
				page->data = frame[i].bm_PageHandle.data;

				bufferFull = false;
				break;
			}

			i += 1;
		}

		if (bufferFull == true)
		{
			int pin = fullBufferPinPage(bm, page, pageNum);

			return pin;
		}
		return return_code;
	}
}

// Statistics Interface

PageNumber *getFrameContents(BM_BufferPool *const bm)
{
	RC return_code = RC_OK;
	PageNumber *frameContents = (PageNumber *)malloc(maxBufferSize * sizeof(PageNumber));

	if (frameContents == NULL)
	{
		// Handle memory allocation failure
		RC_message = "The memory allocation was not initialized";
		return_code = RC_BUFFER_NOT_INIT;
		printError(*RC_message);
	}
	else
	{
		Frame *pageFrame = (Frame *)bm->mgmtData;
		int i = 0;
		while (i < maxBufferSize)
		{
			if ((pageFrame[i].bm_PageHandle).pageNum != -1)
			{
				frameContents[i] = (pageFrame[i].bm_PageHandle).pageNum;
			}
			else
			{
				frameContents[i] = NO_PAGE;
			}

			i += 1;
		}
	}

	return frameContents;
};

bool *getDirtyFlags(BM_BufferPool *const bm)
{
	RC return_code = RC_OK;
	bool *dirtyFlagCounts = (bool *)malloc(maxBufferSize * sizeof(bool));

	if (dirtyFlagCounts == NULL)
	{
		// Handle memory allocation failure
		RC_message = "The memory allocation was not initialized";
		return_code = RC_BUFFER_NOT_INIT;
		printError(*RC_message);
	}
	else
	{
		Frame *pageFrame = (Frame *)bm->mgmtData;

		for (int i = 0; i < maxBufferSize; i++)
		{
			if (pageFrame[i].dirtyCount == 1)
			{
				dirtyFlagCounts[i] = true;
			}
			else
			{
				dirtyFlagCounts[i] = false;
			}
		}
	}

	return dirtyFlagCounts;
};

// Return the number of clients that has pinned the same page
int *getFixCounts(BM_BufferPool *const bm)
{
	RC return_code = RC_OK;
	int *fixCounts = (int *)malloc(maxBufferSize * sizeof(int));

	if (fixCounts == NULL)
	{
		// Handle memory allocation failure
		RC_message = "The memory allocation was not initialized";
		return_code = RC_BUFFER_NOT_INIT;
		printError(*RC_message);
	}
	else
	{
		Frame *frame = (Frame *)bm->mgmtData;

		for (int i = 0; i < maxBufferSize; i++)
		{
			if (frame[i].fixCount != -1)
			{
				fixCounts[i] = frame[i].fixCount;
			}
			else
			{
				fixCounts[i] = NO_PAGE;
			}
		}
	}

	return fixCounts;
};

int getNumReadIO(BM_BufferPool *const bm)
{
	RC return_code = RC_OK;
	if (noOfPagesRead == NULL)
	{
		RC_message = "The variable is not defined";
		return_code = RC_IM_KEY_NOT_FOUND;
		printError(*RC_message);

		return return_code;
	}

	return noOfPagesRead + 1;
};

int getNumWriteIO(BM_BufferPool *const bm)
{
	RC return_code = RC_OK;
	if (noOfPagesWrite == NULL)
	{
		RC_message = "The variable is not defined";
		return_code = RC_IM_KEY_NOT_FOUND;
		printError(*RC_message);

		return return_code;
	}

	return noOfPagesWrite;
};