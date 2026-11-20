#ifndef BUFFER_H
#define BUFFER_H

typedef int buffer_item;
#define BUFFER_SIZE 5

int buffer_init(void);
int insert_item(buffer_item item);
int remove_item(buffer_item *item);

#endif
