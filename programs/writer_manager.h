#ifndef __WRITER_MANAGER__
#define __WRITER_MANAGER__

void write_scheduler_init(const char* file_name, int queue_len, size_t bucket_size);
int write_scheduler_write(unsigned long long index, char* buff, size_t len);
size_t write_scheduler_flush(void);
void write_scheduler_close(void);
void* write_scheduler_get_next_available_memory(void);

#endif
