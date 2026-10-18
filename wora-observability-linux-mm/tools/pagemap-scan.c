/* Read-only PAGEMAP_SCAN evaluation over eight pages owned by this probe.
 * cc -std=c11 -Wall -Wextra -Werror pagemap-scan.c -o pagemap-scan
 * Exit 77 means unsupported. No userfaultfd or write-protection flags are used.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/utsname.h>
#include <unistd.h>

static int probe(int fd, volatile unsigned char *memory, size_t page_size)
{
    uint64_t start = (uintptr_t)memory;
    uint64_t end = start + 8 * page_size;
    uint64_t cursor = start;
    unsigned calls = 0, regions = 0, matched_pages = 0;
    while (cursor < end && calls < 4) {
        struct page_region region = {0};
        struct pm_scan_arg query = {
            .size = sizeof(query),
            .flags = 0,
            .start = cursor,
            .end = end,
            .vec = (uintptr_t)&region,
            .vec_len = 1,
            .max_pages = 2,
            .category_mask = PAGE_IS_PRESENT,
            .return_mask = PAGE_IS_PRESENT | PAGE_IS_SWAPPED | PAGE_IS_FILE,
        };
        calls++;
        int count = ioctl(fd, PAGEMAP_SCAN, &query);
        if (count == -1) {
            if (errno == ENOTTY || errno == EOPNOTSUPP || errno == ENOSYS)
                return 77;
            fprintf(stderr, "PAGEMAP_SCAN: %s\n", strerror(errno));
            return 1;
        }
        if (count > 1 || query.flags != 0 || query.walk_end <= cursor || query.walk_end > end) {
            fputs("invalid scan progress or vector count\n", stderr);
            return 1;
        }
        if (count) {
            if (region.start < cursor || region.start >= region.end || region.end > query.walk_end ||
                (region.start - start) % page_size || (region.end - start) % page_size ||
                (region.end - region.start) / page_size > 2 ||
                !(region.categories & PAGE_IS_PRESENT) || (region.categories & ~query.return_mask)) {
                fputs("invalid scan result\n", stderr);
                return 1;
            }
            matched_pages += (unsigned)((region.end - region.start) / page_size);
            regions++;
        }
        cursor = query.walk_end;
    }
    for (unsigned i = 0; i < 8; i++) {
        if (memory[i * page_size] != 9) {
            fputs("probe contents changed\n", stderr);
            return 1;
        }
    }
    printf("flags=0 range_pages=8 vec_len=1 max_pages=2 calls=%u regions=%u matched_pages=%u complete=%d contents_unchanged=1\n",
           calls, regions, matched_pages, cursor == end);
    return cursor == end && matched_pages == 8 ? 0 : 1;
}

int main(void)
{
    struct utsname kernel;
    if (uname(&kernel) == -1) {
        perror("uname");
        return 1;
    }
    printf("kernel=%s machine=%s\n", kernel.release, kernel.machine);
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0 || page_size > 1024 * 1024) {
        fputs("unsupported base page size\n", stderr);
        return 1;
    }
    size_t size = 8 * (size_t)page_size;
    volatile unsigned char *memory = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if ((void *)memory == MAP_FAILED) {
        perror("mmap");
        return 1;
    }
    for (unsigned i = 0; i < 8; i++)
        memory[i * (size_t)page_size] = 9;
    int fd = open("/proc/self/pagemap", O_RDONLY | O_CLOEXEC);
    int result;
    if (fd == -1) {
        perror("open pagemap");
        result = 1;
    } else {
        result = probe(fd, memory, (size_t)page_size);
        if (close(fd) == -1 && !result) {
            perror("close pagemap");
            result = 1;
        }
    }
    if (munmap((void *)memory, size) == -1 && !result) {
        perror("munmap");
        result = 1;
    }
    if (result == 77)
        fputs("PAGEMAP_SCAN unsupported by running kernel\n", stderr);
    return result;
}
