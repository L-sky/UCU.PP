#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <stdint.h>

struct arr_t { int* arr; int shift; };

void* prefixSumSequential(int*, int, int*);
void* prefixSumParallel(int*, int, const int);
void* prefixSumParallelUP(void*);
void* prefixSumParallelDOWN(void*);
uint64_t highresTimeDelta(void*,void*);

int main(int argc, char **argv){
	const int numCPU = 4;									// set number of computation cores
	srand(42);												// set random seed												
	int power = 15;											
	if(argc == 2) power = atol(argv[1]);					// allow input from console call
	int arr_size = 2 << power; 								// set array size: 2 << 15 means 2 to the power of 16
	int* arr = malloc(arr_size*sizeof(int));
	for(int i=0; i<arr_size; i++) arr[i] = rand() % 10;		// generate data
	
	int* arr_seq_out = calloc(arr_size+1, sizeof(int));		// allocate memory for output 
	int* arr_par_out = calloc((arr_size+1), sizeof(int));
	for(int i=0; i<arr_size; i++) arr_par_out[i] = arr[i];	// preserve input array, parallel make changes in place
	
	struct timespec time_1, time_2, time_3; 				// Run both approaches with time benchmark
	
		clock_gettime(CLOCK_REALTIME, &time_1);
	prefixSumSequential(arr, arr_size, arr_seq_out);
		clock_gettime(CLOCK_REALTIME, &time_2);
	prefixSumParallel(arr_par_out, arr_size, numCPU);
		clock_gettime(CLOCK_REALTIME, &time_3);
	
	uint64_t delta_1 = highresTimeDelta((void*) &time_1, (void*) &time_2);
	uint64_t delta_2 = highresTimeDelta((void*) &time_2, (void*) &time_3);
	
	printf("Sequential took %lu microseconds\n", delta_1);
	printf("Parallel   took %lu microseconds\n", delta_2);
	
	return 0;
}

void* prefixSumSequential(int* arr_in, int arr_size, int* arr_seq_out) {
	for(int i=0; i<arr_size; i++) arr_seq_out[i+1] = arr_seq_out[i] + arr_in[i];
}

void* prefixSumParallel(int* arr, int arr_size, const int numCPU){
	pthread_t threads[numCPU];
	struct arr_t arr_t_in[numCPU];
	
	int depth = 0;
	int shift = 1;
	int n_calls_per_level = 1;
	int i = arr_size;
	while (i >>= 1) ++depth; 
	
	// (UP)
	for(i=0, shift=1, n_calls_per_level=(arr_size>>1);	i<depth; 	i++, shift<<=1, n_calls_per_level>>=1){
		for(int j=0; j<n_calls_per_level; j++) {
			if (j>=numCPU) pthread_join(threads[j%numCPU], NULL);
			arr_t_in[j%numCPU].arr = arr-1+(2*j+1)*shift;
			arr_t_in[j%numCPU].shift = shift;
			pthread_create(&threads[j%numCPU], NULL, prefixSumParallelUP, (void*) &arr_t_in[j%numCPU]);
		}
		for(int k=0; k<numCPU; k++) pthread_join(threads[k], NULL);
	}
	
	// Move last element
	arr[arr_size] = arr[arr_size-1];
	arr[arr_size-1] = 0;
	
	// (DOWN)
	for(i=0, shift>>=1, n_calls_per_level=1;	i<depth; 	i++, shift>>=1, n_calls_per_level<<=1){
		for(int j=0; j<n_calls_per_level; j++) {
			if (j>=numCPU) pthread_join(threads[j%numCPU], NULL);
			arr_t_in[j%numCPU].arr = arr-1+(2*j+1)*shift;
			arr_t_in[j%numCPU].shift = shift;
			pthread_create(&threads[j%numCPU], NULL, prefixSumParallelDOWN, (void*) &arr_t_in[j%numCPU]);
		}
		for(int k=0; k<numCPU; k++) pthread_join(threads[k], NULL);
	}
}

void* prefixSumParallelUP(void* param){
	struct arr_t* arr_t_in = (struct arr_t *) param;
	*(arr_t_in->arr + arr_t_in->shift) = *(arr_t_in->arr + arr_t_in->shift) + *(arr_t_in->arr);
}

void* prefixSumParallelDOWN(void* param){
	struct arr_t* arr_t_in = (struct arr_t *) param;
	int sub_sum = *(arr_t_in->arr + arr_t_in->shift) + *(arr_t_in->arr);
	*(arr_t_in->arr) = *(arr_t_in->arr + arr_t_in->shift);
	*(arr_t_in->arr + arr_t_in->shift) = sub_sum;
}

uint64_t highresTimeDelta(void* par_1, void* par_2){
	struct timespec* time_1 = (struct timespec*) par_1;
	struct timespec* time_2 = (struct timespec*) par_2;
	return((time_2->tv_sec - time_1->tv_sec)*1000000 + (time_2->tv_nsec - time_1->tv_nsec)/1000);
}
