#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

int main(int argc, char * argv[])
{
    struct timeval tv_cur;

    size_t sz=(size_t)2*1024*1024*1024;
    char * buf=(char *)calloc(1,sz);
    if (!buf) return 1;

    int cksum=0;
    while (1)
    {
        gettimeofday(&tv_cur, NULL);
        unsigned long starttime = tv_cur.tv_sec * 1000000 + tv_cur.tv_usec;

        for (size_t i=0; i!=sz; ++i)
            cksum+=buf[i];

        gettimeofday(&tv_cur, NULL);
        unsigned long finishtime = tv_cur.tv_sec * 1000000 + tv_cur.tv_usec;

        printf("Took %lu %d\n", finishtime-starttime, cksum);

    }


}

