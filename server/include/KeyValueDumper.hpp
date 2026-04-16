#ifndef MINIREDIS_DUMP_HPP
#define MINIREDIS_DUMP_HPP
#include <unistd.h>
#include <chrono>
#include <fstream>
#include <openssl/evp.h>

#include "common/Singleton.hpp"
#include "common/File.hpp"
#include "common/SysError.hpp"
#include "spdlog/spdlog.h"

namespace MiniRedis {

    /*
    |--------------------------- Header ----------------------------|
    |   magic(16B) |    version(16B) |  kvCount(8B) | timestamp(8B) |
    |---------------------------- Header MD5 -----------------------|
    |              header_md5(16B)                                  |
    |----------------------------KV Body ---------------------------|
    | key_len(8B) | key_data(...) | value_len(8B) | value_data(...) |
    | key_len(8B) | key_data(...) | value_len(8B) | value_data(...) |
    | ............................................................. |
    |------------------------ KV Body MD5 --------------------------|
    |                   kvbody_md5(16B)                             |
     */

    struct KeyValueDumper : public Singleton<KeyValueDumper> {
        friend class Singleton<KeyValueDumper>;

#pragma pack(push, 1)
        struct DumperHeader {
            char magic[16];
            char version[16];
            uint64_t keyValuePairSize;
            uint64_t timestamp;
        };
#pragma pack(pop)

        struct DumperBodyKeyValueBlock {
            uint64_t keySize;
            void *key;
            uint64_t valueSize;
            void *value;
        };

        struct EvpDigestResult {
            unsigned char digest[EVP_MAX_MD_SIZE];
            unsigned int length;
        };

        static int EVP_DigestUpdateVec(EVP_MD_CTX *context, const iovec* vecs, const unsigned int size) {
            for (int i=0;i<size;i++) {
                const int ret = EVP_DigestUpdate(context, vecs[i].iov_base, vecs[i].iov_len);
                if (1 != ret) {
                    spdlog::error("EVP_DigestUpdate fail");
                    return ret;
                }
            }
            return 1;
        }

        pid_t lastForkedChildProcessPid = -1;
        std::chrono::steady_clock::time_point lastDumperTriggerTimePoint{};
        std::chrono::steady_clock::time_point lastLoaderTriggerTimePoint{};


        KeyValueDumper() = default;

        bool dump(
            const std::string_view& savedBinaryFilePath,
            const std::string_view& version,
            const std::unordered_map<std::string, std::string>& dict,
            const bool asynced) {

            if (asynced && lastForkedChildProcessPid != -1) {
                spdlog::error("has a child process currently executing task.");
                return false;
            }

            lastDumperTriggerTimePoint = std::chrono::steady_clock::now();

            if (asynced) {
                lastForkedChildProcessPid = fork();
                if (lastForkedChildProcessPid < 0) {
                    spdlog::error("{}",getSysLastError("fork rdb process failed"));
                    return false;
                }
                if (lastForkedChildProcessPid > 0) {
                    spdlog::info("fork rdb process started,the child process id is {}",lastForkedChildProcessPid);
                    return true;
                }
            }

            //If asynchronous backup is used, the following tasks will only be executed by the child process

            spdlog::info("do exec key value dump.");
            File binaryFile;
            if (!binaryFile.open(savedBinaryFilePath.data(), O_WRONLY | O_CREAT)) {
                spdlog::error("The binary file of dump cannot be opened for output!");
                binaryFile.close();
                exit(-1);
            }

            DumperHeader header{};
            strncpy(header.magic,"MiniRedisDump",13);
            strncpy(header.version,version.data(),version.size());
            header.keyValuePairSize = dict.size();
            header.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            EVP_MD_CTX *headerDigestContext = EVP_MD_CTX_new();
            if (!headerDigestContext) {
                spdlog::error("Cannot create EVP_MD_CTX");
                exit(-1);
            }
            if (1 != EVP_DigestInit_ex(headerDigestContext, EVP_md5(), nullptr)) {
                spdlog::error("EVP_DigestInit_ex fail");
                EVP_MD_CTX_free(headerDigestContext);
                exit(-1);
            }
            if (1 != EVP_DigestUpdate(headerDigestContext, &header, sizeof(header))) {
                spdlog::error("EVP_DigestUpdate fail");
                EVP_MD_CTX_free(headerDigestContext);
                exit(-1);
            }
            EvpDigestResult headerDigestResult{};
            if (1 != EVP_DigestFinal_ex(headerDigestContext, headerDigestResult.digest, &headerDigestResult.length)) {
                spdlog::error("EVP_DigestFinal_ex fail");
                EVP_MD_CTX_free(headerDigestContext);
                exit(-1);
            }
            binaryFile.write(&header, sizeof(header));
            binaryFile.write(headerDigestResult.digest, headerDigestResult.length);

            EVP_MD_CTX *bodyDigestContext = EVP_MD_CTX_new();
            if (!bodyDigestContext) {
                spdlog::error("Cannot create EVP_MD_CTX");
                exit(-1);
            }
            if (1 != EVP_DigestInit_ex(bodyDigestContext, EVP_md5(), nullptr)) {
                spdlog::error("EVP_DigestInit_ex fail");
                EVP_MD_CTX_free(bodyDigestContext);
                exit(-1);
            }
            for (const auto&[key, value] : dict) {
                DumperBodyKeyValueBlock bodyKeyValueBlock{};
                bodyKeyValueBlock.keySize = key.size();
                bodyKeyValueBlock.key = (void*)(key.data());
                bodyKeyValueBlock.valueSize = value.size();
                bodyKeyValueBlock.value = (void*)value.data();

                iovec digestBuffers[4];
                digestBuffers[0].iov_base = &bodyKeyValueBlock.keySize;
                digestBuffers[0].iov_len = sizeof(bodyKeyValueBlock.keySize);
                digestBuffers[1].iov_base = bodyKeyValueBlock.key;
                digestBuffers[1].iov_len = bodyKeyValueBlock.keySize;
                digestBuffers[2].iov_base = &bodyKeyValueBlock.valueSize;
                digestBuffers[2].iov_len = sizeof(bodyKeyValueBlock.valueSize);
                digestBuffers[3].iov_base = bodyKeyValueBlock.value;
                digestBuffers[3].iov_len = bodyKeyValueBlock.valueSize;

                if (1 != EVP_DigestUpdateVec(bodyDigestContext, digestBuffers, 4)) {
                    spdlog::error("EVP_DigestUpdate fail");
                    EVP_MD_CTX_free(bodyDigestContext);
                    exit(-1);
                }

                binaryFile.write(digestBuffers,4);
            }
            EvpDigestResult bodyDigestResult{};
            if (1 != EVP_DigestFinal_ex(bodyDigestContext, bodyDigestResult.digest, &bodyDigestResult.length)) {
                spdlog::error("EVP_DigestFinal_ex fail");
                EVP_MD_CTX_free(bodyDigestContext);
                exit(-1);
            }
            binaryFile.write(&bodyDigestResult.digest, bodyDigestResult.length);
            if (binaryFile.flush() != 0) {
                binaryFile.close();
                exit(-1);
            }
            binaryFile.close();
            exit(1);
        }

        bool loader(const std::string_view& path,std::unordered_map<std::string, std::string>& outDict) {

            spdlog::info("starting to load database file {}",path);
            lastLoaderTriggerTimePoint = std::chrono::steady_clock::now();

            if (!std::filesystem::exists(path)) {
                spdlog::warn("Database file {} does not exist", path);
                return false;
            }

            File binaryFile;
            if (!binaryFile.open(path.data(), O_RDONLY)) {
                spdlog::error("Cannot open dumped database file {}",path);
                return false;
            }

            DumperHeader header{};
            if (binaryFile.read(&header, sizeof(header)) != sizeof(header)) {
                spdlog::error("Read header failed");
                return false;
            }

            const auto dumpTimestamp = std::chrono::system_clock::time_point(std::chrono::milliseconds(header.timestamp));
            char buf[64];
            const std::time_t tt = std::chrono::system_clock::to_time_t(dumpTimestamp);
            const std::tm tm = *std::localtime(&tt);
            std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
            spdlog::info("Dump timestamp: {}", buf);


            EvpDigestResult headerDigest{};
            if (binaryFile.read(headerDigest.digest, 16) <= 0) {
                spdlog::error("Read header digest failed");
                return false;
            }

            EVP_MD_CTX* ctx = EVP_MD_CTX_new();
            if (!ctx) {
                spdlog::error("EVP_MD_CTX_new failed");
                return false;
            }

            EVP_DigestInit_ex(ctx, EVP_md5(), nullptr);
            EVP_DigestUpdate(ctx, &header, sizeof(header));

            unsigned char calcDigest[EVP_MAX_MD_SIZE];
            unsigned int calcLen = 0;
            EVP_DigestFinal_ex(ctx, calcDigest, &calcLen);

            if (memcmp(calcDigest, headerDigest.digest, calcLen) != 0) {
                spdlog::error("Header MD5 mismatch!");
                EVP_MD_CTX_free(ctx);
                return false;
            }

            EVP_MD_CTX_free(ctx);

            if (strncmp(header.magic, "MiniRedisDump", 13) != 0) {
                spdlog::error("Invalid dump file magic");
                return false;
            }

            spdlog::info("Dump version: {}", header.version);
            spdlog::info("KV count: {}", header.keyValuePairSize);


            EVP_MD_CTX* bodyCtx = EVP_MD_CTX_new();
            EVP_DigestInit_ex(bodyCtx, EVP_md5(), nullptr);
            const std::vector<float> progressPoints = {0.0f, 0.25f, 0.5f, 0.75f, 1.0f};
            size_t progressIndex = 0;
            for (uint64_t i = 0; i < header.keyValuePairSize; ++i) {
                const float progress = (i + 1) / (static_cast<float>(header.keyValuePairSize) * 1.0f);
                if (i!=0 && i == header.keyValuePairSize - 1) {
                    spdlog::info("Loader progress: 100%");
                }else if (progressIndex < progressPoints.size() && progress >= progressPoints[progressIndex]) {
                    spdlog::info("Loader progress: {:.0f}%", progressPoints[progressIndex] * 100);
                    progressIndex++;
                }

                uint64_t keySize;
                if (binaryFile.read(&keySize, sizeof(keySize)) != sizeof(keySize)) {
                    spdlog::error("Read keySize failed");
                    return false;
                }

                std::string key(keySize, '\0');
                if (binaryFile.read(key.data(), keySize) != (ssize_t)keySize) {
                    spdlog::error("Read key failed");
                    return false;
                }

                uint64_t valueSize;
                if (binaryFile.read(&valueSize, sizeof(valueSize)) != sizeof(valueSize)) {
                    spdlog::error("Read valueSize failed");
                    return false;
                }

                std::string value(valueSize, '\0');
                if (binaryFile.read(value.data(), valueSize) != (ssize_t)valueSize) {
                    spdlog::error("Read value failed");
                    return false;
                }

                iovec vecs[4];
                vecs[0] = {&keySize, sizeof(keySize)};
                vecs[1] = {(void*)key.data(), keySize};
                vecs[2] = {&valueSize, sizeof(valueSize)};
                vecs[3] = {(void*)value.data(), valueSize};

                if (1 != EVP_DigestUpdateVec(bodyCtx, vecs, 4)) {
                    spdlog::error("Body digest update failed");
                    return false;
                }

                outDict.emplace(std::move(key), std::move(value));
            }

            EvpDigestResult bodyDigest{};
            if (binaryFile.read(bodyDigest.digest, EVP_MAX_MD_SIZE) <= 0) {
                spdlog::error("Read body digest failed");
                return false;
            }

            unsigned char calcBodyDigest[EVP_MAX_MD_SIZE];
            unsigned int calcBodyLen = 0;
            EVP_DigestFinal_ex(bodyCtx, calcBodyDigest, &calcBodyLen);

            EVP_MD_CTX_free(bodyCtx);

            if (memcmp(calcBodyDigest, bodyDigest.digest, calcBodyLen) != 0) {
                spdlog::error("Body MD5 mismatch!");
                return false;
            }

            spdlog::info("Dump load success");
            return true;
        }

    };
}

#endif //MINIREDIS_DUMP_HPP