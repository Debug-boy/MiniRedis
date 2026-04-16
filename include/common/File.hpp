#ifndef MINIREDIS_FILE_HPP
#define MINIREDIS_FILE_HPP

#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>

namespace MiniRedis {

    class File {
    private:
        int fd;
        off_t offset;
    public:
        File() : fd(-1) ,offset(0){
        }

        explicit File(const char *file_path, const int open_flag = O_RDONLY) : fd(-1), offset(0) {
            this->open(file_path, open_flag);
        }

        ~File() {
            this->close();
        }

        File(const File &) = delete;

        File &operator=(const File &) = delete;

        File(File &&other) noexcept : fd(other.fd), offset(other.offset) {
            other.fd = -1;
            other.offset = 0;
        }

        File &operator=(File &&other) noexcept {
            if (this != &other) {
                if (fd != -1)
                    ::close(fd);
                fd = other.fd;
                offset = other.offset;
                other.fd = -1;
                other.offset = 0;
            }
            return *this;
        }

        bool open(const char *file_path, const int open_flag = O_RDONLY) {
            fd = ::open(file_path, open_flag,0644);
            if (fd == -1) {
                return false;
            }
            offset = 0;
            return true;
        }

        [[nodiscard]] bool isOpen() const {
            return fd != -1;
        }

        [[nodiscard]] off_t getOffset() const {
            return offset;
        }

        ssize_t read(void *buf, const size_t count) {
            if (fd == -1) return -1;
            ssize_t bytes_read = ::read(fd, buf, count);
            if (bytes_read > 0) {
                offset += bytes_read;
            }
            return bytes_read;
        }

        ssize_t read(const struct iovec *iov, const int size) {
            if (fd == -1) return -1;
            const ssize_t bytes_written = ::readv(fd, iov, size);
            if (bytes_written > 0) {
                offset += bytes_written;
            }
            return bytes_written;
        }

        ssize_t write(const void *buffer, const size_t size) {
            if (fd == -1) return -1;
            const ssize_t bytes_written = ::write(fd, buffer, size);
            if (bytes_written > 0) {
                offset += bytes_written;
            }
            return bytes_written;
        }

        ssize_t write(const struct iovec *iov, const int size) {
            if (fd == -1) return -1;
            const ssize_t bytes_written = ::writev(fd, iov, size);
            if (bytes_written > 0) {
                offset += bytes_written;
            }
            return bytes_written;
        }

        off_t seek(const off_t new_offset, const int whence = SEEK_SET) {
            if (fd == -1) return -1;
            const off_t res = ::lseek(fd, new_offset, whence);
            if (res != -1) {
                offset = res;
            }
            return res;
        }

        [[nodiscard]] int flush() const {
            if (fd == -1) return -1;
            return ::fsync(fd);
        }

        void close() {
            if (fd != -1) {
                ::close(fd);
                fd = -1;
                offset = 0;
            }
        }

    };
}

#endif //MINIREDIS_FILE_HPP
