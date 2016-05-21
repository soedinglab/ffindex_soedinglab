#include "mpq.h"
#include <mpi.h>

#include <stdlib.h>
#include <sys/queue.h>

int MPQ_size;
int MPQ_rank;
size_t MPQ_num_jobs;
int MPQ_is_init = 0;

enum {
    TAG_JOB,
    TAG_DONE
};

enum {
    MSG_RELEASE,
    MSG_JOB,
    MSG_DONE
};

int MPQ_Init (int argc, char** argv, const size_t num_jobs)
{
    if (MPQ_is_init == 1)
        return MPQ_ERROR_REINIT;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &MPQ_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &MPQ_size);

    MPQ_num_jobs = num_jobs;

    int workers = MPQ_size - 1;
    if (workers < 1) {
        MPI_Finalize();
        return MPQ_ERROR_NO_WORKERS;
    }

    MPQ_is_init = 1;

    return MPQ_SUCCESS;
}

void MPQ_Worker (MPQ_Payload_t payload, void* env)
{
    if (MPQ_is_init == 0)
        return;

    int message[3];
    while (1) {
        MPI_Recv(message, 3, MPI_INT, MPQ_MASTER, TAG_JOB, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (message[0] == MSG_RELEASE) {
            break;
        }

        payload(env, message[1], message[2]);

        message[0] = MSG_DONE;
        message[1] = 0;
        message[2] = 0;

        MPI_Send(message, 3, MPI_INT, MPQ_MASTER, TAG_DONE, MPI_COMM_WORLD);
    }
}

void MPQ_Master (const size_t split_size)
{
    if (MPQ_is_init == 0)
        return;

    typedef struct idle_workers_s {
        int rank;
        STAILQ_ENTRY(idle_workers_s) entries;
    } idle_workers_t;

    STAILQ_HEAD(idle_workers, idle_workers_s) idle_workers_head;
    STAILQ_INIT(&idle_workers_head);

    size_t num_workers = MPQ_size;
    if (MPQ_num_jobs < MPQ_size) {
        num_workers = MPQ_num_jobs;
    }
\
    idle_workers_t* worker = NULL;
    for (int i = 1; i < num_workers; ++i) {
        worker = malloc(sizeof(idle_workers_t));
        worker->rank = i;
        STAILQ_INSERT_HEAD(&idle_workers_head, worker, entries);
    }

    for (int i = (int)num_workers; i < MPQ_size; ++i) {
        MPQ_Release_Worker(i);
    }

    // FIXME: find a better way to solve this than just completely suppressing splits
    size_t split = split_size;
    if (MPQ_num_jobs < split) {
        split = 1;
    }

    MPI_Status status;
    int message[3];

    size_t remaining_jobs = MPQ_num_jobs;
    size_t start_job = 0;

    while (remaining_jobs > 0 || !STAILQ_EMPTY(&idle_workers_head)) {
        if (remaining_jobs > 0 && !STAILQ_EMPTY(&idle_workers_head)) {
            size_t batch_size = (remaining_jobs < split) ? remaining_jobs : split;

            message[0] = MSG_JOB;
            message[1] = start_job;
            message[2] = start_job + batch_size;

            start_job += batch_size;
            remaining_jobs -= batch_size;

            idle_workers_t* idle_worker = STAILQ_FIRST(&idle_workers_head);
            MPI_Send(message, 3, MPI_INT, idle_worker->rank, TAG_JOB, MPI_COMM_WORLD);

            STAILQ_REMOVE_HEAD(&idle_workers_head, entries);
            free(idle_worker);
        } else {
            MPI_Recv(message, 3, MPI_INT, MPI_ANY_SOURCE, TAG_DONE, MPI_COMM_WORLD, &status);
            switch (message[0]) {
                default:
                case MSG_DONE: {
                    idle_workers_t* idle_worker = malloc(sizeof(idle_workers_t));
                    idle_worker->rank = status.MPI_SOURCE;
                    STAILQ_INSERT_HEAD(&idle_workers_head, idle_worker, entries);
                    break;
                }
            }
        }
    }

    while (!STAILQ_EMPTY(&idle_workers_head)) {
        worker = STAILQ_FIRST(&idle_workers_head);
        STAILQ_REMOVE_HEAD(&idle_workers_head, entries);
        free(worker);
    }

}

void MPQ_Release_Worker (const int worker_rank)
{
    int message[3];
    message[0] = MSG_RELEASE;
    message[1] = 0;
    message[2] = 0;
    MPI_Send(message, 3, MPI_INT, worker_rank, TAG_JOB, MPI_COMM_WORLD);
}

void MPQ_Finalize ()
{
    if (MPQ_is_init == 0)
        return;

    if (MPQ_rank == MPQ_MASTER)
    {
        for (int i = 1; i < MPQ_size; i++)
        {
            MPQ_Release_Worker(i);
        }
    }

    MPI_Finalize();
    MPQ_is_init = 0;
}
