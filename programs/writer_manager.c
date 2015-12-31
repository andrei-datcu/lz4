#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

typedef struct {
    unsigned long long index;
    char* data;
    size_t data_len;
} write_info_t;

static unsigned long long next_index = 0;
static write_info_t* heap;
static size_t heap_len = 0, heap_capacity;
FILE* dst_file;

static void swap(size_t index1, size_t index2)
{
    static write_info_t aux;
    aux = heap[index1];
    heap[index1] = heap[index2];
    heap[index2] = aux;
}

static void heap_up(size_t index)
{
    if (index == 0)
        return;

    size_t parent_index = (index - 1) / 2;
    if (heap[parent_index].index > heap[index].index) {
        swap(index, parent_index);
        heap_up(parent_index);
    }
}

static void heap_dw(size_t index)
{
    size_t chosen_index = index;
    size_t child1_index = 2 * (index + 1),
           child2_index = child1_index - 1;
    if (child1_index < heap_len && heap[child1_index].index < heap[index].index)
        chosen_index = child1_index;

    if (child2_index < heap_len && heap[child2_index].index < heap[chosen_index].index)
        chosen_index = child2_index;

    if (index != chosen_index) {
        swap(index, chosen_index);
        heap_dw(chosen_index);
    }
}

// ADD to heap
// WARNING!!! This assumes there is enough room in the heap
static void add_to_heap(const write_info_t* info)
{
    memcpy(heap + heap_len, info, sizeof(write_info_t));
    heap_len++;
    heap_up(heap_len - 1);
}

static void pop_the_heap(void)
{
    heap_len--;
    swap(0, heap_len);
    heap_dw(0);
}

void write_scheduler_init(const char* file_name, int queue_len, size_t bucket_size)
{
    dst_file = fopen(file_name, "wb");
    assert(dst_file);
    heap = calloc(queue_len, sizeof(write_info_t));
    assert(heap);
    for (int i = 0; i < queue_len; ++i) {
        heap[i].data = malloc(bucket_size);
        assert(heap[i].data);
    }
    heap_capacity = queue_len;
    heap_len = 0;
    next_index = 0;
}

static void write_chunk(const write_info_t* chunk)
{
    assert(fwrite(chunk->data, 1, chunk->data_len, dst_file) == chunk->data_len);
    //free(chunk->data);
}

static void do_write_as_much_as_possible(void)
{
    while (heap_len && heap[0].index == next_index) {
        write_chunk(heap);
        pop_the_heap();
        next_index++;
    }
}

int write_scheduler_write(unsigned long long index, char* buff, size_t len)
{
    static write_info_t wi;

    wi.data = buff;
    wi.data_len = len;
    wi.index = index;
    if (index == next_index) {
        write_chunk(&wi);
        ++next_index;
        do_write_as_much_as_possible();
        return 0;
    }

    if (heap_len  == heap_capacity)
        return -1;

    add_to_heap(&wi);
    do_write_as_much_as_possible();
    return 0;
}


size_t write_scheduler_flush(void)
{
    do_write_as_much_as_possible();
    return heap_len;
}

void write_scheduler_close(void)
{
    write_scheduler_flush();
    fclose(dst_file);
    for (unsigned i = 0; i < heap_capacity; ++i)
        free(heap[i].data);
    free(heap);
}

void* write_scheduler_get_next_available_memory(void)
{
    if (heap_len < heap_capacity)
        return heap[heap_len].data;
    else
        return NULL;
}
