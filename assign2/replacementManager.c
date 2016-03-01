/*Manages the maintanance and initialization of replacement strategy data structures*/
# include "intQueue.c" //TODO fix this so it can find the queue functions
#include "struct_mgr.h"


/*For initialization*/
/*to manage strategies*/


//TODO replace type
replaceData* initBMreplaceData(BM_BufferPool *bm){
    /*Initializes the replacement data for the various strategies*/
    intQueue *empty;
    initQueue(empty, bm->numPages);
	replaceData *data = (replaceData *) malloc(sizeof(replaceData));
    data->FIFOq=empty;
    data->LRUq=empty;
    data->Clockq=empty;
	return data;
}


/*This is to implement various replacement algorithims*/
/*Each of these functions takes a buffer manager and the index of the target page.*/
 int incNIndex(intQueue *q,int n){
	//increments the index N positions
	//if n is greater than the length of the queue, then reduce it
	if(n>q->length)
	{
		return incNIndex(q,n-q->length);
	}
	
	//Otherwise if there is no need to wrap, increment the index
	if (((q->index)+n)<((q->length)-1))
	{
		return (q->index+n);
	}
	
	//if you need to wrap, then wrap
    return (q->index)+n -(q->length);
}
int checkPinned(MgmtData *ref,int location){
	//checks if a given page is pinned
	/*1 for pinned, 0 for unpinned*/
	if ((ref->fixCount[location])>0)
	{
		return 1;
	}
	return 0;
}
int nextUnpinned(MgmtData *ref,intQueue *q){
	/*This function finds the closest unpinned entry to the top of the queue*/
	result=q->index;
	while (checkPinned(ref,result)==1){
		result=incNIndex(q,consecPins);
		consecPins++;
	}
	return result;
}
int replaceNext(MgmtData *ref, intQueue *q, int replacement){
	/*This method takes in a queue,
	pulls the next unpinned item to the top,
	And then enqueues the replacement and returns the deleted page index (queue contents)*/
	int next=nextUnpinned(ref,q);
	if(next>q->index){
		moveToBack(q,next);
		/*Since moveToBack moves the index to the item after the nextUnpinned,
		decrement the index to shift the nextUnpinned to the top of the queue*/
		q->index=decIndex(q);
	}
	return enQueue(q,replacement);
	}

int replaceFIFO(MgmtData *ref,int target){
	return replaceNext(ref,ref->FIFOq,target);
}

/*LRU replacement, uses intQueue of the frameIndex's in order of use */
void updateLRU(int target,BM_BufferPool bm/*TODO add type*/){
    moveToFront(bm->mgmtData->LRUqueue->queue,target);
}
int replaceLRU(MgmtData *ref, int target){
    return replaceNext(ref,ref->LRUq,target);
}

/*Clock replacement, uses a queue on mutable data TODO handle case where target is pinned*/
/*Apply this function upon pinning*/
void updateClock(int target,BM_BufferPool bm/*TODO add type*/){
    bm->mgmtData->Clock->queue[target]=1;
}
int replaceClock(BM_BufferPool bm/*TODO add type*/){
   /*finds the index of the frame to be replaced with the Clock algorithim*/
   /*if frame has been used,
        reset use variable
        increment
        recurse*/
     if(bm->mgmtData->Clockq->queue[bm->mgmtData->]==1){
         bm->mgmtData->Clockq->queue[bm->mgmtData->]=0;
         bm->mgmtData->Clockq->index=incIndex[ bm->mgmtData->Clockq];
         return replaceClock(bm);
     }
     return bm->mgmtData->Clockq->index;
}
