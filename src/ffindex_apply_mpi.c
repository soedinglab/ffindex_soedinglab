/* 
 * FFindex
 * written by Andy Hauser <hauser@genzentrum.lmu.de>,
 * and Milot Mirdita <milot@mirdita.de>
 * Please add your name here if you distribute modified versions.
 *
 * FFindex is provided under the Create Commons license "Attribution-ShareAlike
 * 3.0", which basically captures the spirit of the Gnu Public License (GPL).
 *
 * See:
 * http://creativecommons.org/licenses/by-sa/3.0/
 *
 * ffindex_apply_mpi
 * apply a program to each FFindex entry
 */

#define _GNU_SOURCE 1
#define _LARGEFILE64_SOURCE 1
#define _FILE_OFFSET_BITS 64

#include <limits.h> // PIPE_BUF
#include <stdlib.h> // EXIT_*, system, malloc, free
#include <unistd.h> // pipe, fork, close, dup2, execvp, write, read, opt*

#include <sys/mman.h> // munmap
#include <sys/wait.h> // waitpid
#include <fcntl.h>    // fcntl, F_*, O_*
#include <signal.h>   // sigaction, sigemptyset

#include <getopt.h>   // getopt_long

#include <sys/queue.h> // SLIST_*

#include <spawn.h>     // spawn_*

#include <assert.h>    // assert

#include "ffindex.h"
#include "ffutil.h"
#include "mpq/mpq.h"

char read_buffer[400 * 1024 * 1024];
extern char **environ;

int
ffindex_apply_by_entry(char *data,
                       ffindex_index_t* index, ffindex_entry_t* entry,
                       char* program_name, char** program_argv,
                       FILE* data_file_out, FILE* index_file_out,
                       size_t* offset, int quiet)
{
    int ret = 0;
    int capture_stdout = (data_file_out != NULL && index_file_out != NULL);

    int pipefd_stdin[2];
    int pipefd_stdout[2];

    ret = pipe(pipefd_stdin);
    if (ret != 0)
    {
        fprintf(stderr, "ERROR in pipe stdin!\n");
        perror(entry->name);
        return errno;
    }

    if (capture_stdout)
    {
        ret = pipe(pipefd_stdout);
        if (ret != 0)
        {
            fprintf(stderr, "ERROR in pipe stdout!\n");
            perror(entry->name);
            return errno;
        }

        // Flush so child doesn't copy and also flushes, leading to duplicate output
        fflush(data_file_out);
        fflush(index_file_out);
    }

    posix_spawn_file_actions_t factions;
    posix_spawn_file_actions_init(&factions);
    posix_spawn_file_actions_addclose(&factions, pipefd_stdin[1]);
    posix_spawn_file_actions_adddup2(&factions, pipefd_stdin[0], fileno(stdin));
    if (capture_stdout) {
        posix_spawn_file_actions_addclose(&factions, pipefd_stdout[0]);
        posix_spawn_file_actions_adddup2(&factions, pipefd_stdout[1], fileno(stdout));
    }

    short flags = 0;
    posix_spawnattr_t attr;
    posix_spawnattr_init(&attr);
#ifdef POSIX_SPAWN_USEVFORK
    flags |= POSIX_SPAWN_USEVFORK;
#endif
    posix_spawnattr_setflags(&attr, flags);

    pid_t child_pid;
    int err = posix_spawnp(&child_pid, program_name, &factions, &attr, program_argv, environ);
    if (err)
    {
        fprintf(stderr, "ERROR in fork()\n");
        perror(entry->name);
        return errno;
    }
    else
    {
        // parent writes to and possible reads from child
        int flags = 0;

        // Read end is for child only
        close(pipefd_stdin[0]);

        if (capture_stdout)
        {
            close(pipefd_stdout[1]);
        }

        char *file_data = ffindex_get_data_by_entry(data, entry);

        if (capture_stdout)
        {
            flags = fcntl(pipefd_stdout[0], F_GETFL, 0);
            fcntl(pipefd_stdout[0], F_SETFL, flags | O_NONBLOCK);
        }

        char *b = read_buffer;

        // Write file data to child's stdin.
        ssize_t written = 0;
        // Don't write ffindex trailing '\0'
        size_t to_write = entry->length - 1;
        while (written < to_write)
        {
            size_t rest = to_write - written;
            int batch_size = PIPE_BUF;
            if (rest < PIPE_BUF)
            {
                batch_size = rest;
            }

            ssize_t w = write(pipefd_stdin[1], file_data + written, batch_size);
            if (w < 0 && errno != EPIPE)
            {
                fprintf(stderr, "ERROR in child!\n");
                perror(entry->name);
                break;
            }
            else
            {
                written += w;
            }

            if (capture_stdout)
            {
                // To avoid blocking try to read some data
                ssize_t r = read(pipefd_stdout[0], b, PIPE_BUF);
                if (r > 0)
                {
                    b += r;
                }
            }
        }
        close(pipefd_stdin[1]); // child gets EOF

        if (capture_stdout)
        {
            // Read rest
            fcntl(pipefd_stdout[0], F_SETFL, flags); // Remove O_NONBLOCK
            ssize_t r;
            while ((r = read(pipefd_stdout[0], b, PIPE_BUF)) > 0)
            {
                b += r;
            }
            close(pipefd_stdout[0]);

            ffindex_insert_memory(data_file_out, index_file_out, offset, read_buffer, b - read_buffer, entry->name);
        }

        int status;
        waitpid(child_pid, &status, 0);
        if (!quiet)
        {
            fprintf(stdout, "%s\t%ld\t%ld\t%d\n", entry->name, entry->offset, entry->length, WEXITSTATUS(status));
        }
    }

    return EXIT_SUCCESS;
}


typedef struct worker_splits_s worker_splits_t;

struct worker_splits_s {
    int start;
    int end;
    int status;
    SLIST_ENTRY(worker_splits_s) entries;
};

SLIST_HEAD(worker_splits, worker_splits_s) worker_splits_head;

typedef struct ffindex_apply_mpi_data_s ffindex_apply_mpi_data_t;
struct ffindex_apply_mpi_data_s {
    void	*  data;
    ffindex_index_t* index;
    char*  data_filename_out;
    char*  index_filename_out;
    char*  program_name;
    char** program_argv;
    int    quiet;
};

ffindex_apply_mpi_data_t ffindex_payload_environment;

int ffindex_apply_worker_payload (const size_t start, const size_t end) {
    FILE *data_file_out = NULL;
    if (ffindex_payload_environment.data_filename_out != NULL)
    {
        char data_filename_out_rank[FILENAME_MAX];
        snprintf(data_filename_out_rank, FILENAME_MAX, "%s.%d.%zu.%zu",
                 ffindex_payload_environment.data_filename_out, MPQ_rank, start, end);

        data_file_out = fopen(data_filename_out_rank, "w+");
        if (data_file_out == NULL)
        {
            fferror_print(__FILE__, __LINE__, "ffindex_apply_worker_payload", ffindex_payload_environment.data_filename_out);
            return EXIT_FAILURE;
        }
    }

    FILE *index_file_out = NULL;
    if (ffindex_payload_environment.index_filename_out != NULL)
    {
        char index_filename_out_rank[FILENAME_MAX];
        snprintf(index_filename_out_rank, FILENAME_MAX, "%s.%d.%zu.%zu",
                 ffindex_payload_environment.index_filename_out, MPQ_rank, start, end);

        index_file_out = fopen(index_filename_out_rank, "w+");
        if (index_file_out == NULL)
        {
            fferror_print(__FILE__, __LINE__, "ffindex_apply_worker_payload", ffindex_payload_environment.index_filename_out);
            return EXIT_FAILURE;
        }
    }

    int exit_status = EXIT_SUCCESS;
    size_t offset = 0;
    for (size_t i = start; i < end; i++)
    {
        ffindex_entry_t *entry = ffindex_get_entry_by_index(ffindex_payload_environment.index, i);
        if (entry == NULL)
        {
            perror(entry->name);
            exit_status = errno;
            break;
        }

        int error = ffindex_apply_by_entry(ffindex_payload_environment.data, ffindex_payload_environment.index, entry,
                                           ffindex_payload_environment.program_name,
                                           ffindex_payload_environment.program_argv, data_file_out,
                                           index_file_out, &offset, ffindex_payload_environment.quiet);
        if (error != 0)
        {
            perror(entry->name);
            exit_status = errno;
            break;
        }
    }

    if (index_file_out) {
        fclose(index_file_out);
    }
    if (data_file_out) {
        fclose(data_file_out);
    }

    worker_splits_t* entry = malloc(sizeof(worker_splits_t));
    entry->start = start;
    entry->end = end;
    entry->status = exit_status;
    SLIST_INSERT_HEAD(&worker_splits_head, entry, entries);

    return exit_status;
}

void ffindex_worker_merge_splits(char* data_filename, char* index_filename, int worker_rank, int remove_temporary)
{
    if (!data_filename)
        return;

    if (!index_filename)
        return;

    char merge_command[FILENAME_MAX * 5];
    char tmp_filename[FILENAME_MAX];

    worker_splits_t* entry;
    SLIST_FOREACH(entry, &worker_splits_head, entries) {
        snprintf(merge_command, FILENAME_MAX,
                 "ffindex_build -as -d %s.%d.%d.%d -i %s.%d.%d.%d %s.%d %s.%d",
                 data_filename, worker_rank, entry->start, entry->end,
                 index_filename, worker_rank, entry->start, entry->end,
                 data_filename, worker_rank,
                 index_filename, worker_rank);

        int exit_status = system(merge_command);
        if (exit_status == 0 && remove_temporary)
        {
            snprintf(tmp_filename, FILENAME_MAX, "%s.%d.%d.%d",
                     index_filename, worker_rank, entry->start, entry->end);
            remove(tmp_filename);

            snprintf(tmp_filename, FILENAME_MAX, "%s.%d.%d.%d",
                     data_filename, worker_rank, entry->start, entry->end);
            remove(tmp_filename);
        }
    }
}

void ffindex_merge_splits(char* data_filename, char* index_filename, int splits, int remove_temporary)
{
    if (!data_filename)
        return;

    if (!index_filename)
        return;

    char merge_command[FILENAME_MAX * 5];
    char tmp_filename[FILENAME_MAX];

    for (int i = 1; i < splits; i++)
    {
        snprintf(merge_command, FILENAME_MAX,
                 "ffindex_build -as -d %s.%d -i %s.%d %s %s",
                 data_filename, i, index_filename, i, data_filename, index_filename);

        int ret = system(merge_command);
        if (ret == 0 && remove_temporary)
        {
            snprintf(tmp_filename, FILENAME_MAX, "%s.%d", index_filename, i);
            remove(tmp_filename);

            snprintf(tmp_filename, FILENAME_MAX, "%s.%d", data_filename, i);
            remove(tmp_filename);
        }
    }
}

int ceildiv(int x, int y) {
    assert(x > 0 && y > 0);
    int q = (x + y - 1) / y;
    return (q > 1 ? q : 1);
}

int
process_queue(int argn, char** argv, int parts)
{

    int mpq_status = mpq_status = MPQ_Init(argn, argv,
                                           ffindex_payload_environment.index->n_entries);

    if (mpq_status != MPQ_SUCCESS)
        return mpq_status;


    if (MPQ_rank != MPQ_MASTER) {
        SLIST_INIT(&worker_splits_head);
    }

    MPQ_Payload = ffindex_apply_worker_payload;
    int split_size = ceildiv(ceildiv(ffindex_payload_environment.index->n_entries, MPQ_size), parts);
    MPQ_Main(split_size);

    if (MPQ_rank != MPQ_MASTER)
    {
        ffindex_worker_merge_splits(ffindex_payload_environment.data_filename_out,
                                    ffindex_payload_environment.index_filename_out, MPQ_rank, 1);

        while (!SLIST_EMPTY(&worker_splits_head))
        {
            worker_splits_t* entry = SLIST_FIRST(&worker_splits_head);
            SLIST_REMOVE_HEAD(&worker_splits_head, entries);
            free(entry);
        }
    }


    MPQ_Finalize();

    if (MPQ_rank == MPQ_MASTER)
    {
        ffindex_merge_splits(ffindex_payload_environment.data_filename_out,
                             ffindex_payload_environment.index_filename_out, MPQ_size, 1);
    }

    return MPQ_SUCCESS;
}

void ignore_signal(int signal)
{
    struct sigaction handler;
    handler.sa_handler = SIG_IGN;
    sigemptyset(&handler.sa_mask);
    handler.sa_flags = 0;
    sigaction(signal, &handler, NULL);
}

void usage()
{
    fprintf(stderr,
            "USAGE: ffindex_apply_mpi [-p PARTS] [-d DATA_FILENAME_OUT -i INDEX_FILENAME_OUT] DATA_FILENAME INDEX_FILENAME -- PROGRAM [PROGRAM_ARGS]*\n"
            "\nDesigned and implemented by Andy Hauser <hauser@genzentrum.lmu.de>\n"
            "and Milot Mirdita <milot@mirdita.de>.\n\n"
            "\t[-p PARTS]\t\tSets the effective split size for one batch to be computed for one worker.\n"
            "\t\tThe split size is computed with Total Number of Jobs / Number of Workers / PARTS.\n"
            "\t[-d DATA_FILENAME_OUT]\tFFindex data file where the results will be saved to.\n"
            "\t[-i INDEX_FILENAME_OUT]\tFFindex index file where the results will be saved to.\n"
            "\t[-q]\tSilence the logging of every processed entry.\n"
            "\tDATA_FILENAME\t\tInput ffindex data file.\n"
            "\tINDEX_FILENAME\t\tInput ffindex index file.\n"
            "\tPROGRAM [PROGRAM_ARGS]\tProgram to be executed for every ffindex entry.\n"
            );
}

int main(int argn, char** argv)
{
    ignore_signal(SIGPIPE);

    int exit_status = EXIT_SUCCESS;

    int parts = 10;
    int quiet = 0;
    char *data_filename_out  = NULL;
    char *index_filename_out = NULL;

    static struct option long_options[] =
    {
        {"parts",   required_argument, NULL, 'p'},
        {"data",    required_argument, NULL, 'd'},
        {"index",   required_argument, NULL, 'i'},
        {"quiet",   no_argument,       NULL, 'q'},
        {NULL,      0,                 NULL,  0 }
    };

    int opt;
	while (1)
	{
        int option_index = 0;
        opt = getopt_long(argn, argv, "qp:d:i:", long_options, &option_index);
        if(opt == -1)
            break;

		switch (opt)
		{
            case 'd':
                data_filename_out = optarg;
                break;
            case 'i':
                index_filename_out = optarg;
                break;
            case 'p':
                parts = atoi(optarg);
                break;
            case 'q':
                quiet = 1;
                break;
		}
	}

    const int remaining_arguments = argn - optind;
	if (remaining_arguments < 3)
	{
        if(remaining_arguments < 2)
        {
            fprintf(stderr, "Please specify input data and index file.\n\n");
        }

        fprintf(stderr, "Please specify program to execute.\n\n");

		usage();

        return EXIT_FAILURE;
	}

    if ((!data_filename_out && index_filename_out) || (data_filename_out && !index_filename_out))
    {
        fprintf(stderr, "Please specify both output data and index file.\n\n");

        usage();

        return EXIT_FAILURE;
    }

	char *data_filename = argv[optind++];
	FILE *data_file = fopen(data_filename, "r");
	if (data_file == NULL)
	{
		fferror_print(__FILE__, __LINE__, argv[0], data_filename);
		return EXIT_FAILURE;
	}

	char *index_filename = argv[optind++];
	FILE *index_file = fopen(index_filename, "r");
	if (index_file == NULL)
	{
		fferror_print(__FILE__, __LINE__, argv[0], index_filename);
		exit_status = EXIT_FAILURE;
		goto cleanup_1;
	}

	char *program_name = argv[optind];
	char **program_argv = argv + optind;

	size_t data_size;
	char *data = ffindex_mmap_data(data_file, &data_size);
    if (data == MAP_FAILED)
    {
        fferror_print(__FILE__, __LINE__, "ffindex_mmap_data", index_filename);
        exit_status = EXIT_FAILURE;
        goto cleanup_2;
    }

	ffindex_index_t *index = ffindex_index_parse(index_file, 0);
	if (index == NULL)
	{
		fferror_print(__FILE__, __LINE__, "ffindex_index_parse", index_filename);
		exit_status = EXIT_FAILURE;
		goto cleanup_3;
	}

    ffindex_payload_environment.data = data;
    ffindex_payload_environment.index = index;
    ffindex_payload_environment.program_name = program_name;
    ffindex_payload_environment.program_argv = program_argv;
    ffindex_payload_environment.data_filename_out = data_filename_out;
    ffindex_payload_environment.index_filename_out = index_filename_out;
    ffindex_payload_environment.quiet = quiet;

    int mpq_status = process_queue(argn, argv, parts);
    switch (mpq_status) {
        case MPQ_ERROR_NO_WORKERS:
            fprintf(stderr, "MPQ_Init: Needs at least one worker process.\n");
            return EXIT_FAILURE;
        case MPQ_ERROR_TOO_MANY_WORKERS:
            fprintf(stderr, "MPQ_Init: Too many worker processes.\n");
            return EXIT_FAILURE;
    }

    munmap(index->index_data, index->index_data_size);
    free(index);

  cleanup_3:
	munmap(data, data_size);

  cleanup_2:
    if(index_file)
    {
        fclose(index_file);
    }

  cleanup_1:
    if(data_file)
    {
        fclose(data_file);
    }

	return exit_status;
}
