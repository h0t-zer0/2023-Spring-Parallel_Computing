#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <assert.h>
#include <time.h>

/*
    function name : compare
    param : const void *p1
            const void *p2
    function : used for qsort()
*/
int compare(const void *p1, const void *p2)
{
    return (*(int *)p1 - *(int *)p2);  // 升序 返回值 <0 p1 排在 p2 前面
}

/*
    function name : Merge
    param : int *a
            int p
            int q
            int r
    function : merge a[p] ~ a[q] and a[q + 1] ~ a[r]
*/
void Merge(int *a, int p, int q, int r)
{
    int i, j, k;
    int n1 = q - p + 1;
    int n2 = r - q;
    int L[n1];
    int R[n2];
    for (i = 0; i < n1; i++)
        L[i] = a[p + i];
    for (j = 0; j < n2; j++)
        R[j] = a[q + j + 1];
    i = 0, j = 0, k = p;
    while (i < n1 && j < n2)
    {
        if (L[i] <= R[j])
        {
            a[k++] = L[i++];
        }
        else
        {
            a[k++] = R[j++];
        }
    }
    while (i < n1)
    {
        a[k++] = L[i++];
    }
    while (j < n2)
    {
        a[k++] = R[j++];
    }
}

/*
    function name : MergeSort
    param : int *a
            int p
            int r
    function : merge sort a[p] ~ a[r] recursively 
*/
void MergeSort(int *a, int p, int r)
{
    if (p < r)
    {
        int q = (p + r) / 2;
        MergeSort(a, p, q);
        MergeSort(a, q + 1, r);
        Merge(a, p, q, r);
    }
}

/*
    function name : getRandomArray
    param : int *arr
            int arr_len
            int seed
            int range
    function : fill array arr (length = arr_len) with random int number from 0 to range - 1, with seed = seed 
*/
void getRandomArray(int *arr, int arr_len, int seed, int range)
{
    srand(seed);
    for (int i = 0; i < arr_len; i++)
        arr[i] = rand() % range;
}

/*
    function name : debug
    param : int *array
            int len
    function : print array arr
*/
void debug(int *arr, int arr_len)
{
    for (int i = 0; i < arr_len; i++)
        printf("%d,", arr[i]);
    printf("\n");
}

// main function
int main(int argc, char *argv[])
{
    int seed = 888;

    if (argc != 2 && argc != 3)  // 输入参数数目必须为 2 或 3，第一个为程序名 第二个为排序数组长度，（第三个为随机数种子，默认为 888）
    {
        if (argc == 1)
        {
            printf("\n Error : please input array length ! \n");
        }
        else
        {
            printf("\n Error : too many inputs ! \n");
        }
        exit(EXIT_FAILURE);
    }
    if (argc == 3)  // 获取输入的随机种子，默认初始化为 888
    {
        seed = atoi(argv[2]);
    }

    int len = atoi(argv[1]);  // 获取排序数组长度

    int array[len];  // 带排序数组
    int brray[len];  // 检测数组
    int result[len];  // 接受再次分发的数据
    int nprocs;  // 处理器数
    int myid;  // 进程 id
    int group;  // 平均每组有多少个数据（不含余数）
    int group_len;  // 每个组实际的数据量，除最后一组外，group_len = group，最后一组 group_len = group + 余数
    int mod;  // 余数
    double begin_time, end_time, interval;  // 用于进程计时

    int i, index;

    // 随机产生带排序数组
    getRandomArray(array, len, 888, 1000);
    getRandomArray(brray, len, 888, 1000);

    // 开始mpi
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);
    MPI_Status status;

    assert(nprocs * nprocs <= len);

    // if(myid == 0)
    // {
    //     debug(array, len);
    //     debug(brray, len);
    // }

    // 此处开始进程独立计时
    begin_time = MPI_Wtime();

    if (nprocs == 1) {  // 单进程直接 qsort
        // MergeSort(array, 0, len - 1);
        qsort(array, len, sizeof(int), compare);
        end_time = MPI_Wtime ();
        interval = end_time - begin_time;
    }

    else {  // 多进程
        group = len / nprocs;  // 每个处理器分得多少个数据（最后一个加余数）
        mod = len % nprocs;  // 取余数全部塞给最后一个处理器
        
        int sample[nprocs * nprocs];  // 用于记录所有的采样数据
        int pivot[nprocs];  // 用于储存每个进程选出的 nprocs 个主元
        
        // 均匀划分 局部排序
        if (myid != nprocs - 1) {  // 非最后一个处理器，对数据进行局部快排
            qsort(array + myid * group, group, sizeof(int), compare);
            group_len = group;
        }
        else {  // 最后一个处理器处理更多的数据
            qsort(array + myid * group, group + mod, sizeof(int), compare);
            group_len = group + mod;
        }

        // debug(array, len);

        // 正则采样
        for (i = 0; i < nprocs; i++)  // 每个进程从 group 个数据中采样 p 个数据作为 pivot
            sample[i + myid * nprocs] = array[myid * group + i * group / nprocs];

        // 采样到的样本全部放到 0 号进程 tag = 100 + myid
        if (myid == 0)
        {
            for (i = 1; i < nprocs; i++)
                MPI_Recv(sample + nprocs * i, nprocs, MPI_INT, i, 100 + i, MPI_COMM_WORLD, &status);
        }
        else
        {
            MPI_Send(sample + myid * nprocs, nprocs, MPI_INT, 0, 100 + myid, MPI_COMM_WORLD);
        }

        // 将采样的主元进行快排
        if (myid == 0)
        {
            qsort(sample, nprocs * nprocs, sizeof(int), compare);
            for (i = 0; i < nprocs - 1; i++) 
                pivot[i] = sample[(i + 1) * nprocs];  // 步长 nprocs 取出 nprocs - 1 个主元
        // 分发主元
            for(i = 1;i < nprocs; i++)
                MPI_Send(pivot, nprocs, MPI_INT, i, 200 + i, MPI_COMM_WORLD);
        }

        MPI_Barrier(MPI_COMM_WORLD);  // 保证所有进程都已经执行至此再进行主元接受操作

        // 接收主元
        if(myid != 0)
            MPI_Recv(pivot, nprocs, MPI_INT, 0, 200 + myid, MPI_COMM_WORLD, &status);

        // 进行主元划分
        index = 0;

        int partitionSize[nprocs];  // 记录每个处理器的 group 个数据被分为 p 段后，每一段各自的长度
        for(i = 0; i < nprocs; i++)  // 初始化各段长度为 0
            partitionSize[i] = 0;
        
        for(i = 0; i < group_len; i++) {
            if (array[i + myid * group] > pivot[index])
                index++;
            if (index == nprocs) {
                partitionSize[nprocs - 1] = group_len - i + 1;
                break;
            }
            partitionSize[index]++;
        }

        // 广播分组 先发送各个段的长度 从进程 i 发送的 j 块由进程 j 接收，并放置在接收缓冲区的第 i 个块中
        int newSize[nprocs];
        MPI_Alltoall(partitionSize, 1, MPI_INT, newSize, 1, MPI_INT, MPI_COMM_WORLD);

        // 计算位移
        int sendIndex[nprocs];
        int recvIndex[nprocs];

        sendIndex[0] = 0;  // 实际发送位置位移
        recvIndex[0] = 0;  // 实际接收位置位移
        for(i = 1; i < nprocs; i++) {
            sendIndex[i] = sendIndex[i - 1] + partitionSize[i - 1];
            recvIndex[i] = recvIndex[i - 1] + newSize[i - 1];
        }
        // 计算总长度
        int totalSize = 0;
        for(i = 0; i < nprocs; i++)
            totalSize += newSize[i];


        // 每个处理器发送数据给其他所有处理器，且每个处理发送的数据长度都不同
        // 故有长度数组和位移数组
        MPI_Alltoallv(array + myid * group, partitionSize, sendIndex, MPI_INT, 
            result, newSize, recvIndex, MPI_INT, MPI_COMM_WORLD);
        
        // 此时每个数组自己内部的 result 都使用了 0 ~ totalSize - 1

        // 归并排序
        MergeSort(result, 0, totalSize-1);

        // 将所有进程的 totalSize 汇总到 0号进程的 recvIndex
        MPI_Gather(&totalSize, 1, MPI_INT, recvIndex, 1, MPI_INT, 0, MPI_COMM_WORLD);

        int recvDisp[nprocs];
        recvDisp[0];
        for(i = 1;i < nprocs;i++)
            recvDisp[i] = recvIndex[i - 1] + recvDisp[i - 1];

        // 将每个进程的 result 里的 totalSize 个数据 按照位移 recvDisp 汇总到 0号进程的 array
        MPI_Gatherv(result, totalSize, MPI_INT, array, recvIndex, recvDisp, MPI_INT, 0, MPI_COMM_WORLD);

        // 程序逻辑执行结束
        end_time = MPI_Wtime ();
        interval = end_time - begin_time;
        MPI_Barrier(MPI_COMM_WORLD);
    }

    // 找出最长的进程耗时作为运行时间
    double t_all[nprocs];
    MPI_Gather(&interval, 1, MPI_DOUBLE, t_all, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    if (myid == 0)
    {
        double t_max = t_all[0];
        for (i = 1; i < nprocs; i++)
        {
            if (t_max < t_all[i]) 
                t_max = t_all[i];
        }
        printf ("run time = %lf ms\n", t_max * 1000);

        double s_begin, s_end, s_interval;

        s_begin = MPI_Wtime();
        MergeSort(brray, 0, len - 1);
        s_end = MPI_Wtime();
        s_interval = s_end - s_begin;
        // printf ("t_serial = %lf ms\n", s_interval * 1000);

        for (int i = 0; i < len; i++) {
            if (array[i] != brray[i])
            {
                printf("result wrong ! \n");
                break;
            }
        }
        printf("result correct ! \n");
    }

    // if(myid == 0)
    // {
    //     debug(array, len);
    //     debug(brray, len);
    // }

    MPI_Finalize();
    return 0;
}
