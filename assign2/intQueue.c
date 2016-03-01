/*This file contains functions definining the intQueue data structure which will be used in multiple replacement strategies.*/
/*Also included are functions useful to the data structure.*/
typedef struct intQueue{
    int *queue[];/*Please check this working without specified length*/
    int index;
    int length;
}

void initArray(int size, int *queue){
    /*initializes an array of default values*/
    int progress=0;
    const default=-1;
    while(progress<size-1){
        queue[progress]=default
        progress++;
    }
}
void initQueue(int length,intQueue *queue){
    int emptyArray[length];/*TODO update for pointers*/
    initArray(length, emptyArray)
    queue->queue=emptyArray;
    queue->index=0;
    queue->length=length;
}

/*Index increments*/
int incIndex(FifoQueue *target){
    /*if not at end of array, return index+1,
      if at edge of array, reset to beginning.*/
    if ((target->index)<(target->length-1)){return (target->index+1);}
    return 0;
}
int decIndex(FifoQueue *target){
    /*if not at beginning of array, return index-1,
      if at edge of array, reset to beginning.*/
    if ((target->index)>0){return (target->index-1);}
    return target->length-1;
}

/*adding to queue */
int enQueue(Queue *target,int new){
    /*This function returns the front of the queue: the element to be deleted.
        it also increments the index
        it also enqueues the given element at the back of the queue*/
    int result=target->queue[queue->index];
    queue->index=nextIndex(queue);
    return result;
}

/*These functions are for MoveToBack*/
int countMoves(Queue *q, int targetIndex){
    /*This function counts the number of moves needed to update the queue
        */
     if(targetIndex>q->Index){
        /*No need to wrap around the array*/
        return targetIndex-(q->Index);
    }
    else{
        /*Account for wrapping around the array*/
        return length-(q->Index-targetIndex);
    }
}

void moveToBack(Queue *q, int targetIndex){
    /*This function moves the argument target to the back of the input queue*/
    int remainingMoves;
    remainingMoves=countMoves(q,targetIndex);
    
    //Store the value to be enqueued
    int temp=q->queue[targetIndex];
    
    /*Then, cascade replace each element with its prior element,
      until there are no more replacements to make*/
    replacementTarget=targetIndex;
    replacementSource=decIndex(targetIndex);
    
    /*
    Termination Function: remaining moves
    Decrement function: remainingMoves--;
    exit condition: No more moves;
    invariant:???*/
    while (remainingMoves>0){
       q->queue[replacementTarget]=q->queue[replacementSource];
       replacementTarget=decIndex(replacementTarget);
       replacementSource=decIndex(replacementSource);
       remainingMoves--;
    }
    replacementSource=temp;
    
    /*Then 
    q->queue[replacementTarget]=q->queue[replacementSource];
    q->index=incIndex(q->index);
}
