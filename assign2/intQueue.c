/*This file contains functions definining the intQueue data structure which will be used in multiple replacement strategies.*/
/*Also included are functions useful to the data structure.*/

#include "intQueue.h"
#include <unistd.h>
#include<stdio.h>
#include <stdlib.h>
/*
#include <stdlib.h>
typedef struct intQueue{
    int index;
    int length;
    int *queue;
} intQueue;
*/

void initArray(int size, int *queue){
    /*
 * Function Name: initArray
 *
 * Description:
 *	Fills an array with default values
 *
 * Parameters:
 *      int size:     The length of the array
 *	int *queue:   An array of integers to be populated
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Bill Molchan   Initialization
 */
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
void initQueue(intQueue *queue,int length){
	   /*
 * Function Name: initQueue
 *
 * Description:
 *	Fills an intQueue with default values
 *
 * Parameters:
 *      int length:     The length of the queue
 *	int *queue:     The queue to be populated
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Bill Molchan   Initialization
 */
    int emptyArray[length];/*TODO update for pointers*/
    initArray(length, emptyArray);

    //queue->queue = (int **) malloc (sizeof(int *)*length);

    queue->queue = (int *) malloc (sizeof(int)*length);
    int i=0;
    for(i;i<length;i++){
        queue->queue[i]=0;
    }

    queue->index=0;
    queue->length=length;
}

/*Index increments*/
int incIndex(intQueue *target){
 /*
 * Function Name: incIndex
 *
 * Description:
 *	Gives the int second from the top of the queue
 *
 * Parameters:
 *      intQueue target: the intQueue to read from
 *
 * Return:
 *	returns the item that would be removed in 2 enqueues
 
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Bill Molchan   Initialization
 */
    /*if not at end of array, return index+1,
      if at edge of array, reset to beginning.*/
    if ((target->index)<(target->length-1)){return (target->index+1);}
    return 0;
}
int decIndex(intQueue *target){
  /*
 * Function Name: decIndex
 *
 * Description:
 *	Gives most recently added item to the queue
 *
 * Parameters:
 *      intQueue target: the intQueue to read from
 *
 * Return:
 *	returns the item most recently added.
 
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Bill Molchan   Initialization
 */
 /*if not at beginning of array, return index-1,
      if at edge of array, reset to beginning.*/
    if ((target->index)>0){return (target->index-1);}
    return target->length-1;
}

int relDecIndex(intQueue *q, int where){
    /*
 * Function Name: relDecIndex
 *
 * Description:
 *	Non-destructively returns the element entered before the 'where'
 *
 * Parameters:
 *      intQueue target: the intQueue to read from
 *	int	 where:  The number of positions into the queue to look
 *
 * Return:
 *	returns the item that was 1 entry after 'where'
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Bill Molchan   Initialization
 */
    /*if not at beginning of array, return where-1,
      if at edge of array, reset to beginning.*/
    if (where>0){return (where-1);}
    return q->length-1;
}
int incNIndex(intQueue *q,int n){
	 /*
 * Function Name: incNIndex
 *
 * Description:
 *	Non-destructively returns the element 'n' positions from the front of the queue
 *
 * Parameters:
 *      intQueue target: the intQueue to read from
 *	int	 n:  The number of positions into the queue to look
 *
 * Return:
 *	returns the item that was added 'n' entries ago, '0' for 'n' returns the top of the queue
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Bill Molchan   Initialization
 */
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
/*adding to queue */
int enQueue(intQueue *target,int new){
	   /*
 * Function Name: enQueue
 *
 * Description:
 *	Adds an element to a queue and returns the element removed from the queue
 *
 * Parameters:
 *      intQueue target:     The queue to be edited
 *		int new:   An array of integers to be populated
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Bill Molchan   Initialization
 */
    /*This function returns the front of the queue: the element to be deleted.
        it also increments the index
        it also enqueues the given element at the back of the queue*/

    int result=target->queue[target->index];
    target->index=incIndex(target);

    return result;
}

/*These functions are for MoveToBack*/
int countMoves(intQueue *q, int targetIndex){
	   /*
 * Function Name: countMoves
 *
 * Description:
 * Counts the number of moves until the top of the queue is at targetIndex
 *
 * Parameters:
 *      intQueue q:         The queue to be studied
 *	int targetIndex:    A position in the queue <where the top of the queue will move>
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Bill Molchan   Initialization
 */
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
      /*
 * Function Name: moveToBack
 *
 * Description:
 *	Takes an element and a Queue.
 *		takes the element out of the queue and inserts it at the end
 *		elements between the targetIndex and the current index are pushed forward 1 level in the queue
 *
 * Parameters:
 *      intQueue q:        The queue to be edited
 *	int targetIndex:   The index of the item to be moved to the back of the queue
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Bill Molchan   Initialization
 */
    
    int remainingMoves;

    remainingMoves=countMoves(q,targetIndex);

    //Store the value to be enqueued
    int temp=q->queue[targetIndex];

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
    q->index=incIndex(q);
}
void moveToFront(intQueue *q, int targetIndex){
      /*
 * Function Name: moveToFront
 *
 * Description:
 *	Takes an element and a Queue.
 *		takes the element out of the queue and inserts it at the beginning
 *		elements between the targetIndex and the current index are pushed backward 1 level in the queue
 *
 * Parameters:
 *      intQueue q:        The queue to be edited
 *	int targetIndex:   The index of the item to be moved to the front of the queue
 *
 * History:
 *      Date        Name            Content
 *      ----------  --------------  ---------------------
 *      02/20/2016  Bill Molchan   Initialization
 */
    moveToBack(q,targetIndex);
    q->index=decIndex(q);
}
