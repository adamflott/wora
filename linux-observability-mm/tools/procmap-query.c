/* Read-only evaluation probe; requires Linux 6.11+ UAPI headers.
 * cc -std=c11 -Wall -Wextra -Werror procmap-query.c -o procmap-query
 * Exit 77 means the running kernel does not support PROCMAP_QUERY.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <linux/fs.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/utsname.h>
#include <unistd.h>

static int probe(int fd, const char *mode, uint64_t filter)
{
    uint64_t cursor = 0;
    unsigned calls = 0, mappings = 0;
    int complete = 0, name_limit = 0;
    while (calls < 64) {
        char name[256] = {0};
        struct procmap_query query = {
            .size = sizeof(query),
            .query_flags = PROCMAP_QUERY_COVERING_OR_NEXT_VMA | filter,
            .query_addr = cursor,
            .vma_name_size = sizeof(name),
            .vma_name_addr = (uintptr_t)name,
        };
        calls++;
        if (ioctl(fd, PROCMAP_QUERY, &query) == -1) {
            if (errno == ENOTTY || errno == EOPNOTSUPP || errno == ENOSYS)
                return 77;
            if (errno == ENOENT) {
                complete = 1;
                break;
            }
            if (errno == E2BIG) {
                name_limit = 1;
                break;
            }
            fprintf(stderr, "PROCMAP_QUERY: %s\n", strerror(errno));
            return 1;
        }
        if (query.vma_start >= query.vma_end || query.vma_end <= cursor ||
            query.vma_page_size == 0 || query.vma_name_size > sizeof(name) ||
            (query.vma_name_size && name[query.vma_name_size - 1] != '\0') ||
            (filter == PROCMAP_QUERY_VMA_READABLE && !(query.vma_flags & filter))) {
            fputs("invalid query result\n", stderr);
            return 1;
        }
        cursor = query.vma_end;
        mappings++;
    }
    printf("mode=%s calls=%u mappings=%u complete=%d name_limit=%d\n",
           mode, calls, mappings, complete, name_limit);
    return 0;
}

int main(void)
{
    struct utsname kernel;
    if (uname(&kernel) == -1) {
        perror("uname");
        return 1;
    }
    printf("kernel=%s machine=%s\n", kernel.release, kernel.machine);
    int fd = open("/proc/self/maps", O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        perror("open maps");
        return 1;
    }
    int result = probe(fd, "all", 0);
    if (!result)
        result = probe(fd, "readable", PROCMAP_QUERY_VMA_READABLE);
    if (!result)
        result = probe(fd, "file_backed", PROCMAP_QUERY_FILE_BACKED_VMA);
    if (close(fd) == -1 && !result) {
        perror("close maps");
        result = 1;
    }
    if (result == 77)
        fputs("PROCMAP_QUERY unsupported by running kernel\n", stderr);
    return result;
}
