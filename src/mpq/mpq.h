#include <stddef.h>

#define MPQ_MASTER 0

extern int MPQ_rank;
extern int MPQ_size;

enum {
    MPQ_SUCCESS = 0,
    MPQ_ERROR_NO_WORKERS,
    MPQ_ERROR_TOO_MANY_WORKERS,
    MPQ_ERROR_UNKNOWN
};

int MPQ_Init (int argc, char** argv, const size_t num_jobs, const size_t split_size);

void MPQ_Main ();

void MPQ_Finalize ();

typedef int (*MPQ_Payload_t) (const size_t start, const size_t end);
MPQ_Payload_t MPQ_Payload;