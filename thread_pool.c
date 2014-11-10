#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <stdio.h>
#include "thread_pool.h"
#include "util.h"
/**
 *  @struct threadpool_task
 *  @brief the work struct
 *
 *  Feel free to make any modifications you want to the function prototypes and structs
 *
 *  @var function Pointer to the function that will perform the task.
 *  @var argument Argument to be passed to the function.
 */

#define MAX_THREADS 20
#define STANDBY_SIZE 8

//this global variable is for setting detach state
pthread_attr_t attr; 

typedef struct {
    void (*function)(void *);
    void *argument;
    void *next;
} pool_task_t;


struct pool_t {
  pthread_mutex_t queue_lock;
  pthread_mutex_t fetch_lock;

  pthread_cond_t notify;

  int goAhead;
  pthread_mutex_t goAhead_lock;

  pthread_t *threads;
  pool_task_t *queue_head;
  pool_task_t *queue_tail;
  int thread_count;

  int task_queue_size_limit;
  int task_queue_temp_size; //current size of queue
};

static void *thread_do_work(void *pool);


/*
 * Create a threadpool, initialize variables, etc
 *
 */
pool_t *pool_create(int queue_size, int num_threads)
{
    pool_t* retVal;
    retVal = (pool_t*) malloc(sizeof(pool_t));
    retVal->thread_count = num_threads;
    retVal->task_queue_size_limit = queue_size;
    retVal->task_queue_temp_size= 0;
    retVal->queue_head = NULL;
    retVal->queue_tail = NULL;
    retVal->threads = (pthread_t*)malloc(num_threads* sizeof(pthread_t));

    //initialzie the mutexes
    if(pthread_mutex_init(&(retVal->queue_lock), NULL) != 0)
        printf("Error: unsuccessful mutex initializing for queue_lock\n");
    if(pthread_mutex_init(&(retVal->fetch_lock), NULL) != 0)
        printf("Error: unsuccessful mutex initializing for fetch_lock\n");
    if(pthread_cond_init(&(retVal->notify), NULL) != 0)
        printf("Error: unsuccessful condition variavle initializing for notify\n");

    retVal->goAhead = 1;
    if(pthread_mutex_init(&(retVal->goAhead_lock), NULL) != 0)
        printf("Error: unsuccessful mutex initializing for goAhead lock\n");

    //initialize attr
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    
    int i;
    for(i = 0;i < num_threads; i++)
    {
        pthread_create(&(retVal->threads[i]),&attr, &thread_do_work, (void*)retVal);

    }
    pthread_attr_destroy(&attr);
    return retVal;
}


/*
 * Add a task to the threadpool
 * This function should be called directly by http_server.c to replace the handle_connection()
 * pass in fucntion pointer to hanlde_connection()
 * pass in connfd as argument
 */
int pool_add_task(pool_t *pool, void (*function)(void *), void *argument)
{
    printf("debug: adding task to queue\n");
    int err = 0;
    //get the lock
    pthread_mutex_lock(&(pool->fetch_lock));
    //add task to queue
    //if queue is full, we return directly and skip the task.
    if(pool->task_queue_temp_size == pool->task_queue_size_limit)
    {
        printf("debug: the queue is full. we ignore the task\n");
        free(argument);//because we malloc it in the http_server loop
        pthread_mutex_unlock(&(pool->fetch_lock));
        return err;
    }
    

    //get the queue lock, since we need to manitipulate the queue
    pthread_mutex_lock(&(pool->queue_lock));
    //increase tempSize
    pool->task_queue_temp_size++;
   
    //declare and initialize newly added pool_task_t
    pool_task_t* newJob;
    newJob = (pool_task_t*)malloc(sizeof(pool_task_t));
    newJob->function = function;
    newJob->argument = argument;
    newJob->next = NULL;

    //physically enqueue it to the tail of linkedlist
    if(pool->task_queue_temp_size == 1)  
    {
        //we alread increase the tempSize, so, it is an empty queue
        //this job is added into a empty queue
        if(pool->queue_tail != NULL)
            printf("Error: queue_tail is NOT NULL when queue is empty\n");
        if(pool->queue_head != NULL)
            printf("Error: queue_head is NOT NULL when queue is empty\n");

        printf("debug: add the newJob into an empty queue\n");
        pool->queue_tail = newJob;
        pool->queue_head = newJob;
    }
    else
    {
        //general case
        pool->queue_tail->next = (void*)newJob;
        pool->queue_tail = newJob;
    }
    pthread_mutex_unlock(&(pool->queue_lock));

    //pthread_cond_broadcast and unlock fetch lock
    pthread_cond_signal(&(pool->notify));
    pthread_mutex_unlock(&(pool->fetch_lock));
    return err;
}



/*
 * Destroy the threadpool, free all memory, destroy treads, etc
 *
 */
int pool_destroy(pool_t *pool)
{
    int err = 0;
    
    //wake up all worker threas
    //first reset the goAhead flag
    pthread_mutex_lock(&(pool->goAhead_lock));
    pool->goAhead = 0;
    pthread_mutex_unlock(&(pool->goAhead_lock));

    //then send all cancellation request
    
    int i;
    
    //ready to wake up all the threads.
    for(i = 0; i< pool->thread_count; i++)
    {
        pthread_mutex_lock(&(pool->fetch_lock));
            pthread_cond_broadcast(&(pool->notify));
        pthread_mutex_unlock(&(pool->fetch_lock));
    }
    //join all work threads.
    for(i = 0; i<pool->thread_count; i++)
    {
        if(pthread_join((pool->threads)[i], NULL) != 0)
        {
            printf("Error: thread_join return NON-zero value\n");
        }
        printf("debug: join a thread\n");
    }
    //everything is well, we deallocate the pool
    //we need to free all jobs that still in the queue
    while(pool->queue_head != NULL)
    {
        pool_task_t* temp = pool->queue_head;
        pool->queue_head = pool->queue_head->next;
        free(temp->argument);
        free(temp);
    }
    return err;
}



/*
 * Work loop for threads. Should be passed into the pthread_create() method.
 * this function will actively fetch job from the working queue
 */
static void *thread_do_work(void *pool)
{ 
    pool_t* tp; 
    tp = (pool_t*) pool;

    while(1) {
    //always check the goAhead int, if it's reset to 0 by destroy function, then 
    //we break the loop
        pthread_mutex_lock(&(tp->goAhead_lock));
        if(tp->goAhead != 1)
        {
            printf("debug: bread the loop because of flag\n");
            break;
        }
        pthread_mutex_unlock(&(tp->goAhead_lock));
       
        //lock must be taken to wait on conditional variable
        pthread_mutex_lock(&(tp->fetch_lock));
        while(tp->queue_head == NULL)
        {
            //thread is waiting for job to be added to empty queue
            pthread_cond_wait(&(tp->notify), &(tp->fetch_lock));
            pthread_mutex_lock(&(tp->goAhead_lock));
            if(tp->goAhead != 1)
            {
                //break the loop because of the flag and broadcast
                pthread_mutex_unlock(&(tp->goAhead_lock));
                pthread_mutex_unlock(&(tp->fetch_lock));
                pthread_exit(NULL);
                return(NULL);
            }
            pthread_mutex_unlock(&(tp->goAhead_lock));

        }
        //ready to fetch work from queue
        //the thread will actively grab work from queue, insead of been called 
        //this function never ends until the end of functions
        
        //grab task from the queue
        pool_task_t* newJob;
        pthread_mutex_lock(&(tp->queue_lock));
        (tp->task_queue_temp_size)--;
        newJob = tp->queue_head;
        tp->queue_head = (pool_task_t*)tp->queue_head->next;

        if(tp->task_queue_temp_size == 0)
        {//after this job, the queue is empty
            //we are fetching the only job from queue
            if(tp->queue_head != NULL)
                printf("Error: head is NOT set to NULL when empty\n");
            tp->queue_tail = NULL;
        }
        else
        {
            //after fetching , there are still some jobs in queue
            if(tp->task_queue_temp_size == 1 && tp->queue_head != tp->queue_tail)
                printf("Error: only one job left, but tail!= head\n");
        }
        pthread_mutex_unlock(&(tp->queue_lock));
    //unlock fetch_lock for others
        pthread_mutex_unlock(&(tp->fetch_lock));
        //start the task
        if(newJob->function != handle_connection)
            printf("Error: function is NOT handle_connection\n");
        (*(newJob->function))(newJob->argument);

        //after we finish the work, we free the pool_task_t structure
        free(newJob->argument);
        free(newJob);
    }
    pthread_exit(NULL);
    return(NULL);
}
