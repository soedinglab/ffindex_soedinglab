#include <stddef.h>

#define MPQ_MASTER 0

extern int MPQ_rank;
extern int MPQ_size;

void MPQ_Init (int argc, char** argv);

void MPQ_Main (const size_t num_jobs, const size_t split_size);

void MPQ_Finalize ();

typedef int (*MPQ_Payload_t) (const size_t start, const size_t end, const void* environment);
MPQ_Payload_t MPQ_Payload;

void* MPQ_Payload_Environment;