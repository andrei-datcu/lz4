#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <string.h>
#include "writer_manager.h"

int main(int argc, char **argv)
{
    assert(argc == 3);
    srand(time(NULL));

    const int splits = 8;
    const size_t BLOCK_SIZE = 128;
    const size_t CHUNK_SIZE = BLOCK_SIZE / splits;
    char buff[BLOCK_SIZE];

    write_scheduler_init(argv[2], splits, CHUNK_SIZE);
    FILE* fin = fopen(argv[1], "rb");
    assert(fin);
    for (int counter = 0;; ++counter) {
        if (fread(buff, 1, BLOCK_SIZE, fin) != BLOCK_SIZE)
            break;
        unsigned int used_set = 0;
        for (int i = 0; i < splits; ++i) {
            int split;
            do {
                split = rand() % splits;
            } while (used_set & (1 << split));
            used_set |= (1 << split);
            char* split_buf = write_scheduler_get_next_available_memory();
            memcpy(split_buf, buff + split * CHUNK_SIZE, CHUNK_SIZE);
            assert(write_scheduler_write(counter * splits + split, split_buf, CHUNK_SIZE) == 0);
        }
    }
    write_scheduler_close();
    fclose(fin);
    return 0;
}
