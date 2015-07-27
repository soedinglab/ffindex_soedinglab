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
 * ffindex_apply
 * apply a program to each FFindex entry
 */

#define _GNU_SOURCE 1
#define _LARGEFILE64_SOURCE 1
#define _FILE_OFFSET_BITS 64

#include <limits.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <mpi.h>

#include "ffindex.h"
#include "ffutil.h"

#define MASTER_RANK 0

char read_buffer[400 * 1024 * 1024];

int
ffindex_apply_by_entry(char *data, ffindex_index_t * index,
					   ffindex_entry_t * entry, char *program_name,
					   char **program_argv, FILE * data_file_out, FILE * index_file_out, size_t * offset)
{
	int ret = 0;
	int capture_stdout = (data_file_out != NULL);

	int pipefd_stdin[2];
	ret = pipe(pipefd_stdin);
	if (ret != 0)
	{
		fprintf(stderr, "ERROR in pipe stdin!\n");
		perror(entry->name);
		return errno;
	}

	int pipefd_stdout[2];
	if (capture_stdout)
	{
		ret = pipe(pipefd_stdout);
		if (ret != 0)
		{
			fprintf(stderr, "ERROR in pipe stdout!\n");
			perror(entry->name);
			return errno;
		}
	}

	// Flush so child doesn't copy and also flushes, leading to duplicate
	// output
	fflush(data_file_out);
	fflush(index_file_out);

	pid_t child_pid = fork();

	if (child_pid == 0)			// child
	{
		close(pipefd_stdin[1]);
		if (capture_stdout)
		{
			fclose(data_file_out);
			fclose(index_file_out);
			close(pipefd_stdout[0]);
		}
		// Make pipe from parent our new stdin
		int newfd_in = dup2(pipefd_stdin[0], fileno(stdin));
		if (newfd_in < 0)
		{
			fprintf(stderr, "ERROR in dup2 in %d %d\n", pipefd_stdin[0], newfd_in);
			perror(entry->name);
		}
		close(pipefd_stdin[0]);

		if (capture_stdout)
		{
			int newfd_out = dup2(pipefd_stdout[1], fileno(stdout));
			if (newfd_out < 0)
			{
				fprintf(stderr, "ERROR in dup2 out %d %d\n", pipefd_stdout[1], newfd_out);
				perror(entry->name);
			}
			close(pipefd_stdout[1]);
		}
		// exec program with the pipe as stdin
		ret = execvp(program_name, program_argv);

		// never reached on success of execvp
		if (ret == -1)
		{
			perror(program_name);
			return errno;
		}
	}
	else if (child_pid > 0)		// parent
	{
		// parent writes to and possible reads from child

		int flags = 0;

		// Read end is for child only
		close(pipefd_stdin[0]);

		if (capture_stdout)
		{
			close(pipefd_stdout[1]);

			flags = fcntl(pipefd_stdout[0], F_GETFL, 0);
			fcntl(pipefd_stdout[0], F_SETFL, flags | O_NONBLOCK);
		}

		char *filedata = ffindex_get_data_by_entry(data, entry);

		// Write file data to child's stdin.
		ssize_t written = 0;
		size_t to_write = entry->length - 1;	// Don't write ffindex
		// trailing '\0'
		char *b = read_buffer;
		while (written < to_write)
		{
			size_t rest = to_write - written;
			int batch_size = PIPE_BUF;
			if (rest < PIPE_BUF)
			{
				batch_size = rest;
			}

			ssize_t w = write(pipefd_stdin[1], filedata + written,
							  batch_size);
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
		close(pipefd_stdin[1]);	// child gets EOF

		if (capture_stdout)
		{
			// Read rest
			fcntl(pipefd_stdout[0], F_SETFL, flags);	// Remove O_NONBLOCK
			ssize_t r;
			while ((r = read(pipefd_stdout[0], b, PIPE_BUF)) > 0)
			{
				b += r;
			}
			close(pipefd_stdout[0]);

			ffindex_insert_memory(data_file_out, index_file_out, offset, read_buffer, b - read_buffer, entry->name);

			// make sure the data is actually written so ffindex_build sees the output
			fflush(data_file_out);
			fflush(index_file_out);
		}

		int status;
		waitpid(child_pid, &status, 0);
		if (WIFEXITED(status))
		{
			fprintf(stdout, "%s\t%zd\t%zd\t%i\n", entry->name, entry->offset, entry->length, WEXITSTATUS(status));
		}
	}
	else						// fork failed
	{
		fprintf(stderr, "ERROR in fork()\n");
		perror(entry->name);
		return errno;
	}
	return EXIT_SUCCESS;
}

void usage()
{
	fprintf(stderr,
			"USAGE: ffindex_apply_mpi -d DATA_FILENAME_OUT -i INDEX_FILENAME_OUT DATA_FILENAME INDEX_FILENAME -- PROGRAM [PROGRAM_ARGS]*\n"
			"\nDesigned and implemented by Andy Hauser <hauser@genzentrum.lmu.de>.\n");
}

void ignore_signal(int sig)
{
	struct sigaction handler;
	handler.sa_handler = SIG_IGN;
	sigemptyset(&handler.sa_mask);
	handler.sa_flags = 0;
	sigaction(sig, &handler, NULL);
}

void ffindex_merge_splits(char *data_filename, char *index_filename, int splits)
{
	if (!data_filename)
		return;

	if (!index_filename)
		return;

	char merge_command[FILENAME_MAX * 5];
	char tmp_filename[FILENAME_MAX];

	for (int i = 0; i < splits; i++)
	{
		snprintf(merge_command, FILENAME_MAX,
				 "ffindex_build -as %s %s -d %s.%d -i %s.%d",
				 data_filename, index_filename, data_filename, i, index_filename, i);

		int ret = system(merge_command);
		if (ret == 0)
		{
			snprintf(tmp_filename, FILENAME_MAX, "%s.%d", index_filename, i);
			remove(tmp_filename);

			snprintf(tmp_filename, FILENAME_MAX, "%s.%d", data_filename, i);
			remove(tmp_filename);
		}
	}
}

int main(int argn, char **argv)
{
#if MPI_VERSION < 3
	fprintf(stderr, "Warning: The MPI version does not support asynchronous barriers. This means that finished MPI threads might busy wait and consume 100%% of the CPU doing nothing (Depending on your MPI implementation).\n");
#endif
	int exit_status = EXIT_SUCCESS;

	int mpi_rank, mpi_num_procs;

	MPI_Init(&argn, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &mpi_num_procs);


	char *data_filename_out = NULL, *index_filename_out = NULL;

	int opt;
	while ((opt = getopt(argn, argv, "d:i:")) != -1)
	{
		switch (opt)
		{
		case 'd':
			data_filename_out = optarg;
			break;
		case 'i':
			index_filename_out = optarg;
			break;
		}
	}

	if (argn - optind < 3)
	{
		fprintf(stderr, "Not enough arguments %d.\n", optind - argn);
		usage();
		exit_status = EXIT_FAILURE;
		goto cleanup;
	}

	char *data_filename = argv[optind++];
	FILE *data_file = fopen(data_filename, "r");
	if (data_file == NULL)
	{
		fferror_print(__FILE__, __LINE__, argv[0], data_filename);
		exit_status = EXIT_FAILURE;
		goto cleanup;
	}

	char *index_filename = argv[optind++];
	FILE *index_file = fopen(index_filename, "r");
	if (index_file == NULL)
	{
		fferror_print(__FILE__, __LINE__, argv[0], index_filename);
		exit_status = EXIT_FAILURE;
		goto cleanup;
	}

	// Setup one output FFindex for each MPI process
	FILE *data_file_out = NULL;
	if (data_filename_out != NULL)
	{
		char data_filename_out_rank[FILENAME_MAX];
		snprintf(data_filename_out_rank, FILENAME_MAX, "%s.%d", data_filename_out, mpi_rank);

		data_file_out = fopen(data_filename_out_rank, "w+");
		if (data_file_out == NULL)
		{
			fferror_print(__FILE__, __LINE__, argv[0], data_filename_out);
			exit_status = EXIT_FAILURE;
			goto cleanup_1;
		}
	}

	FILE *index_file_out = NULL;
	if (index_filename_out != NULL)
	{
		char index_filename_out_rank[FILENAME_MAX];
		snprintf(index_filename_out_rank, FILENAME_MAX, "%s.%d", index_filename_out, mpi_rank);

		index_file_out = fopen(index_filename_out_rank, "w+");
		if (index_file_out == NULL)
		{
			fferror_print(__FILE__, __LINE__, argv[0], index_filename_out);
			exit_status = EXIT_FAILURE;
			goto cleanup_1;
		}
	}

	char *program_name = argv[optind];
	char **program_argv = argv + optind;

	size_t data_size;
	char *data = ffindex_mmap_data(data_file, &data_size);

	ffindex_index_t *index = ffindex_index_parse(index_file, 0);
	if (index == NULL)
	{
		fferror_print(__FILE__, __LINE__, "ffindex_index_parse", index_filename);
		exit_status = EXIT_FAILURE;
		goto cleanup_2;
	}

	// Ignore SIGPIPE
	ignore_signal(SIGPIPE);


	// calculate range splits for the different mpi workers
	size_t batch_size, left_over, range_start = 0, range_end = 0;

	batch_size = index->n_entries / mpi_num_procs;
	left_over = index->n_entries % mpi_num_procs;

	if (batch_size > 0)
	{
		range_start = mpi_rank * batch_size;
		range_end = range_start + batch_size;

		// the last worker will handle the left over entries
		// this can result in the last worker to have to process mpi_num_procs - 1 tasks more then the rest
		// this is usually alright because the number of tasks is a lot bigger than the number of workers
		if (mpi_rank == mpi_num_procs - 1)
		{
			range_end += left_over;
		}
	}
	else
	{
		// we have less jobs than we have workers
		if (mpi_rank < left_over)
		{
			range_start = mpi_rank;
			range_end = mpi_rank + 1;
		}
	}

	size_t offset = 0;
	for (size_t i = range_start; i < range_end; i++)
	{
		ffindex_entry_t *entry = ffindex_get_entry_by_index(index, i);
		if (entry == NULL)
		{
			perror(entry->name);
			exit_status = errno;
			goto cleanup_2;
		}

		int error = ffindex_apply_by_entry(data, index, entry,
										   program_name,
										   program_argv, data_file_out,
										   index_file_out, &offset);
		if (error != 0)
		{
			perror(entry->name);
			exit_status = errno;
			goto cleanup_2;
		}
	}

  cleanup_2:
	munmap(data, data_size);
	if(index_file_out) {
	  fclose(index_file_out);
	}
	if(data_file_out) {
	  fclose(data_file_out);
	}

  cleanup_1:
	fclose(index_file);
	fclose(data_file);

  cleanup: ;
#if MPI_VERSION >= 3
	// MPI_Barrier will busy-wait in some MPI implementations,
	// leading to 100% cpu usage.
	// The async barrier will circumvent this problem.
	int flag = 0;
	MPI_Request request;
	MPI_Ibarrier(MPI_COMM_WORLD, &request);
    
	while (flag == 0) {
        MPI_Test(&request, &flag, MPI_STATUS_IGNORE);
		// 1 second
		usleep(1000000);
    }
#else
	MPI_Barrier(MPI_COMM_WORLD);
#endif

	MPI_Finalize();

	if (exit_status == EXIT_SUCCESS && mpi_rank == MASTER_RANK)
	{
		ffindex_merge_splits(data_filename_out, index_filename_out, mpi_num_procs);
	}
	return exit_status;
}
