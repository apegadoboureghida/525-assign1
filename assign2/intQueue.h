//
// Created by drow on 1/03/16.
//

#ifndef ASSIGN2_INITQUEUE_H
#define ASSIGN2_INITQUEUE_H

#endif //ASSIGN2_INITQUEUE_H

typedef struct intQueue{

    int index;
    int length;
    int *queue;
} intQueue;

void initArray(int size, int *queue);

void initQueue(intQueue *queue,int length);

int incIndex(intQueue *target);
int decIndex(intQueue *target);
int relDecIndex(intQueue *q, int where);
int enQueue(intQueue *target,int new);
int countMoves(intQueue *q, int targetIndex);
void moveToBack(intQueue *q, int targetIndex);