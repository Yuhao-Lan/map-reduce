#include<stdio.h> 
#include<string.h> 
#include<pthread.h> 
#include<stdlib.h> 
#include<unistd.h> 
  
pthread_t tid[3]; 
pthread_mutex_t lock; 
  
void* trythis(void *arg) 
{ 
    //pthread_mutex_lock(&lock); 
  
    unsigned long i = 0; 
    long tid;
    tid = (long)arg;
    printf("\n Job %ld has started\n", tid); 
  
    for(i=0; i<(0xFFFFFFFF);i++); 
  
    printf("\n Job %ld has finished\n", tid); 
  
    //pthread_mutex_unlock(&lock);
    pthread_exit(NULL);
    // return NULL; 
} 
  
int main(void) 
{ 
    int i = 0; 
    int error; 
  
    if (pthread_mutex_init(&lock, NULL) != 0) 
    { 
        printf("\n mutex init has failed\n"); 
        return 1; 
    } 
  
    while(i < 3) 
    { 
        error = pthread_create(&(tid[i]), NULL, &trythis, (void*) i); 
        if (error != 0) 
            printf("\nThread can't be created :[%s]", strerror(error)); 
        i++; 
    } 
  
    // pthread_join(tid[0], NULL); 
    // pthread_join(tid[1], NULL); 
    // pthread_join(tid[2], NULL); 
    // pthread_join(tid[3], NULL); 
    // pthread_join(tid[4], NULL); 
    // pthread_mutex_destroy(&lock); 
  
    return 0; 
}