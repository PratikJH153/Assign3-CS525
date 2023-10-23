#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "buffer_mgr.h"
#include "replacement_mgr_strat.h"
#include "storage_mgr.h"


extern void initializeVariables(Frame *newFrame, Frame *frame, int index){
	newFrame[index].bm_PageHandle.data = frame->bm_PageHandle.data;
	newFrame[index].bm_PageHandle.pageNum = frame->bm_PageHandle.pageNum;
	newFrame[index].dirtyCount = frame->dirtyCount;
	newFrame[index].fixCount = frame->fixCount;
}

extern void FIFO(BM_BufferPool *const bm, Frame *frame, int noOfPagesRead, int noOfPagesWrite, int maxBufferSize)
{
	Frame *pageFrame = (Frame *) bm->mgmtData;
	
	int frontIndex = noOfPagesRead % maxBufferSize;

	int i = 0;
	while(i < maxBufferSize)
	{
		if(pageFrame[frontIndex].fixCount == 0)
		{
			if(pageFrame[frontIndex].dirtyCount == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[frontIndex].bm_PageHandle.pageNum, &fh, pageFrame[frontIndex].bm_PageHandle.data);
				
				noOfPagesWrite += 1;
			}
			
			initializeVariables(pageFrame, frame, frontIndex);
			break;
		}
		else
		{
			if (frontIndex % maxBufferSize == 0){
				frontIndex = 0;
			} else {
				frontIndex += 1;
			}
		}

		i += 1;
	}
}

extern void LRU(BM_BufferPool *const bm, Frame *frame, int maxBufferSize, int noOfPagesWrite)
{	
	Frame *pageFrame = (Frame *) bm->mgmtData;
	int leastHitIndex = 0;
	int leastHitRef = 0;

	int i = 0;

	while(i < maxBufferSize)
	{
		if(pageFrame[i].fixCount == 0)
		{
			leastHitIndex = i;
			leastHitRef = pageFrame[i].hit;
			break;
		}

		i += 1;
	}	

	for(int i = leastHitIndex + 1; i < maxBufferSize; i++)
	{
		if(pageFrame[i].hit < leastHitRef)
		{
			leastHitIndex = i;
			leastHitRef = pageFrame[i].hit;
		}
	}

	if(pageFrame[leastHitIndex].dirtyCount == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastHitIndex].bm_PageHandle.pageNum, &fh, pageFrame[leastHitIndex].bm_PageHandle.data);
		
		noOfPagesWrite += 1;
	}
	
	initializeVariables(pageFrame, frame, leastHitIndex);
}


extern void CLOCK(BM_BufferPool *const bm, Frame *page, int clockPointer, int maxBufferSize, int noOfPagesWrite)
{	
	Frame *pageFrame = (Frame *) bm->mgmtData;
	while(true)
	{
		if (clockPointer % maxBufferSize == 0){
			clockPointer = 0;
		}

		if(pageFrame[clockPointer].hit == 0)
		{
			if(pageFrame[clockPointer].dirtyCount == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[clockPointer].bm_PageHandle.pageNum, &fh, pageFrame[clockPointer].bm_PageHandle.data);
				
				noOfPagesWrite += 1;
			}
			
			initializeVariables(pageFrame, page, clockPointer);

			clockPointer += 1;
			break;	
		}
		else
		{
			clockPointer += 1;
			pageFrame[clockPointer].hit = 0;		
		}
	}
}
