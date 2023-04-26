#include <iostream>
#include <omp.h>
#include <ctime>

using namespace std;

int partition(int* arr, int start, int end) {
    int temp = arr[start];
    while (start < end) {
        while (start < end && arr[end] >= temp) end--;
        arr[start] = arr[end];
        while(start < end && arr[start] <= temp) start++;
        arr[end] = arr[start];
    }
    arr[start] = temp;
    return start;
}

void quickSort(int* arr, int start, int end) {
    if (start < end) {
        int pos = partition(arr, start, end);
        quickSort(arr, start, pos - 1);
        quickSort(arr, pos + 1, end);
    }
}

void quickSort_parallel(int* arr, int start, int end) {
    if (start < end) {
        // cout << "[" << omp_get_thread_num() << " fork]" << endl;
        int pos = partition(arr, start, end);
        if (end - start + 1 < 1000) {  // 数组长度小于 1000 执行串行
            quickSort_parallel(arr, start, pos - 1);
            quickSort_parallel(arr, pos + 1, end);
        }
        else {
            #pragma omp parallel sections
            {
                #pragma omp section
                {
                    // cout << "thread number forked: " << omp_get_thread_num() << endl;
                    quickSort_parallel(arr, start, pos - 1);
                }

                #pragma omp section
                {
                    // cout << "thread number forked: " << omp_get_thread_num() << endl;
                    quickSort_parallel(arr, pos + 1, end);
                }
            }
        }
    }
}

void debug(int *arr, int len)
{
    for (int i = 0; i < len; i++) {
        cout << arr[i] << " ";
    }
    cout << endl;
}

int main(int argc, char* argv[]) {

    int len = 1000;
    int nthreads = 2;
    int seed = 0;

    if (argc == 2) {
        nthreads = atoi(argv[1]);
    }

    if (argc == 3)
    {
        nthreads = atoi(argv[1]);
        len = atoi(argv[2]);
    }

    if (argc == 4)
    {
        nthreads = atoi(argv[1]);
        len = atoi(argv[2]);
        seed = atoi(argv[3]);
    }

    cout << "len : " << len << "    " << "nthreads : " << nthreads << "    " << "seed : " << seed << endl;
    

    double time_sta, time_end;
    double omp_time_sta, omp_time_end;
    int arr[len], brr[len];

    srand(seed);
    for (int i = 0; i < len; i++) {
        arr[i] = rand();
        brr[i] = arr[i];
    }
    
    
    time_sta = omp_get_wtime();
    quickSort(arr, 0, len - 1);
    time_end = omp_get_wtime();
    cout << "Runtime without parallel : " << (time_end - time_sta) * 1000 << "ms" << endl;

    omp_set_num_threads(nthreads);
    omp_time_sta = omp_get_wtime();
    quickSort_parallel(brr, 0, len - 1);
    omp_time_end = omp_get_wtime();
    cout << "Runtime with parallel : " << (omp_time_end - omp_time_sta) * 1000 << "ms" << endl;

    cout << "Speedup = " << (time_end - time_sta) / (omp_time_end - omp_time_sta) << endl;

    // debug(arr, len);
    // debug(brr, len);

    for (int i = 0; i < len; i++) {  // 结果合法性检查
        if (arr[i] != brr[i]) {
            cout << "Result Worng !" << endl;
            return 1;
        }
    }

    cout << "Result Correct !" << endl;

    return 0;
}
