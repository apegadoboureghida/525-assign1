/*This file contains functions definining the intQueue data structure which will be used in multiple replacement strategies.*/
/*Also included are functions useful to the data structure.*/
typedef struct intQueue{

    int index;
    int length;
    int **queue;/*Please check this working without specified length*/
} intQueue;

void initArray(int size, int *queue){
    /*initializes an array of default values*/
    int progress=0;
    int def=-1;
    /*Precondition: queue is an array of size "size", progress is 0
      Termination Function: number of unassigned items
      Loop Invariant: all values before "progress" have been intitialized
      Termination dec: assign value to current location, progress++;
      postCondition: The array has been instantiated with default values*/
    while(progress<size-1){
        queue[progress]=def;
        progress++;
    }
}
void initQueue(int length,intQueue *queue){
    int emptyArray[length];/*TODO update for pointers*/
    initArray(length, emptyArray);
    queue->queue = (int **) malloc (sizeof(int *)*length);
    queue->index=0;
    queue->length=length;
}

/*Index increments*/
int incIndex(intQueue *target){
    /*if not at end of array, return index+1,
      if at edge of array, reset to beginning.*/
    if ((target->index)<(target->length-1)){return (target->index+1);}
    return 0;
}
int decIndex(intQueue *target){
    /*if not at beginning of array, return index-1,
      if at edge of array, reset to beginning.*/
    if ((target->index)>0){return (target->index-1);}
    return target->length-1;
}

int relDecIndex(intQueue *q, int where){
    /*if not at beginning of array, return where-1,
      if at edge of array, reset to beginning.*/
    if (where>0){return (where-1);}
    return q->length-1;
}

/*adding to queue */
int enQueue(intQueue *target,int new){
    /*This function returns the front of the queue: the element to be deleted.
        it also increments the index
        it also enqueues the given element at the back of the queue*/
    int result=*target->queue[target->index];
    target->index=nextIndex(target);
    return result;
}

/*These functions are for MoveToBack*/
int countMoves(intQueue *q, int targetIndex){
    /*This function counts the number of moves needed to update the queue
        */
     if(targetIndex>q->index){
        /*No need to wrap around the array*/
        return targetIndex-(q->index);
    }
    else{
        /*Account for wrapping around the array*/
        return q->length-(q->index-targetIndex);
    }
}

void moveToBack(intQueue *q, int targetIndex){
    /*This function moves the argument target to the back of the input queue*/
    int remainingMoves;
    remainingMoves=countMoves(q,targetIndex);
    
    //Store the value to be enqueued
    int temp=*q->queue[targetIndex];
    
    /*Then, cascade replace each element with its prior element,
      until there are no more replacements to make*/
    int replacementTarget=targetIndex;
    int replacementSource=relDecIndex(q,targetIndex);


    /*
    Precondition: Replacement target/source are both in queue
    Termination Function: remaining moves
    Decrement function: remainingMoves--;
    invariant:The queue past replacementTarget is in the correct order
    loopCondition: There is more data to be copied
    postcondition: The queue is ordered everywhere except possibly the top.;
    */
    while (remainingMoves>0){
       q->queue[replacementTarget]=q->queue[replacementSource];
       replacementTarget=relDecIndex(q, replacementTarget);
       replacementSource=relDecIndex(q, replacementSource);
       remainingMoves--;
    }
    replacementSource=temp;
    
    /*Then enqueue the last element and move the front of the queue*/
    q->queue[replacementTarget]=q->queue[replacementSource];
    q->index=incIndex(q->index);
}
