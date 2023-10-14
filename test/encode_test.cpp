////
//// Created by wwx1250863 on 2023/9/25.
////
#include <iostream>
#include <vector>
#include <util/zstd.h>

#include <iostream>
#include <random>
#include <cassert>
#include <cstring>

constexpr size_t kBufferSize = 4 * 1024;

// 生成0到255之间的随机整数
class RandomNumberGenerator {
public:
    RandomNumberGenerator() : generator(), distribution(-600, 3600) {}

    // 生成0到255之间的随机整数
    int GenerateRandomNumber() {
        return distribution(generator);
    }

private:
    std::random_device rd;  // 随机数种子
    std::mt19937 generator; // Mersenne Twister 生成器
    std::uniform_int_distribution<int> distribution; // 均匀分布在0到255之间
};


struct Buffer {
    size_t len_ {0};
    char buf_[kBufferSize];
};


// 编码uint32_t为Varint格式，返回编码后的字节数
size_t EncodeVarint(uint32_t value, char* buffer) {
    size_t encodedBytes = 0;
    while (value >= 0x80) {
        buffer[encodedBytes] = static_cast<char>((value & 0x7F) | 0x80);
        value >>= 7;
        encodedBytes++;
    }
    buffer[encodedBytes] = static_cast<char>(value);
    encodedBytes++;
    return encodedBytes;
}

// 解码Varint格式为uint32_t，返回解码后的值和使用的字节数
uint32_t DecodeVarint(const char* buffer, size_t& bytesRead) {
    uint32_t result = 0;
    size_t shift = 0;
    bytesRead = 0;
    while (true) {
        char byte = buffer[bytesRead];
        result |= (static_cast<uint32_t>(byte) & 0x7F) << shift;
        shift += 7;
        bytesRead++;
        if ((byte & 0x80) == 0) {
            break;
        }
    }
    return result;
}

Buffer *GenerateRawData () {
    auto* buffer = new Buffer();
    buffer->len_ = kBufferSize;
    RandomNumberGenerator rnd;
    auto *uint_p = reinterpret_cast<uint32_t *>(buffer->buf_);
    auto cnt = kBufferSize / sizeof(uint32_t);
    for (uint64_t i = 0; i < cnt ;i++) {
        uint_p[i] = rnd.GenerateRandomNumber();
    };
    // for (uint64_t i = 0; i < cnt ;i++) {
    //     uint_p[i] = -600 + i;
    // };
    return buffer;
}

uint32_t ZigZagEncode(int32_t value) {
    return (static_cast<uint32_t>(value) << 1) ^ static_cast<uint32_t>(value >> 31);
}

int32_t ZigZagDecode(uint32_t value) {
    return static_cast<int32_t>((value >> 1) ^ -(static_cast<int32_t>(value) & 1));
}


Buffer* CompressWithVarint(Buffer *src) {
    auto dest = new Buffer();
    auto cnt = kBufferSize / sizeof(uint32_t);
    auto *src_p = reinterpret_cast<uint32_t *>(src->buf_);
    auto *dest_p = dest->buf_;
    for (uint64_t i = 0; i < cnt; i++) {
        auto val = ZigZagEncode(src_p[i]);
//        auto val = src_p[i];
        auto bytes = EncodeVarint(val, dest_p);
        dest_p += bytes;
        dest->len_ += bytes;
    }
    return dest;
}

Buffer *DecompressVarint(Buffer *src) {
    auto dest = new Buffer();
    size_t bytes;
    auto *dst_p = reinterpret_cast<uint32_t *>(dest->buf_);
    auto *src_p = src->buf_;
    int idx = 0;
    while (src_p != src->buf_ + src->len_) {
       auto val = DecodeVarint(src_p, bytes);
       dst_p[idx++] = ZigZagDecode(val);
//       dst_p[idx++] = val;
       src_p += bytes;
       dest->len_ += sizeof(uint32_t);
    }
    return dest;
}

Buffer* CompressWithZSTD(Buffer *src) {
    auto dest = new Buffer();
    auto rc = ZSTD_compress(dest->buf_, kBufferSize, src->buf_, src->len_, 8);
    if (ZSTD_isError(rc)) {
       std::cerr << "error :" << ZSTD_getErrorName(rc) << std::endl;
        assert(0);
    }
    dest->len_ = rc;
    return dest;
}

Buffer *DecompressZSTD(Buffer *src) {
    auto dest = new Buffer();
    auto rc =  ZSTD_decompress(dest->buf_, kBufferSize, src->buf_, src->len_);
    assert(!ZSTD_isError(rc));
    dest->len_ = rc;
    return dest;
}

bool Validate(Buffer *src, Buffer *result) {
    return ::strncmp(src->buf_, result->buf_, kBufferSize) == 0;
}

#include <chrono>
int main() {
    auto data = GenerateRawData();
    {
        // 获取程序开始时间点
        auto start = std::chrono::high_resolution_clock::now();
        auto compressed = CompressWithZSTD(data);
        std::cout << "ZSTD compress ratio : " <<  compressed->len_ * 1.0 / data->len_ << std::endl;
        auto decompressed = DecompressZSTD(compressed);
        // 获取程序结束时间点
        auto end = std::chrono::high_resolution_clock::now();
        // 计算时间差
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        // 打印程序运行时间
        std::cout << "Time used: " << duration.count() << " us" << std::endl;
        assert(decompressed->len_ == data->len_);
        assert(Validate(data, decompressed));
        delete compressed;
        delete decompressed;
    }
    {
        // 获取程序开始时间点
        auto start = std::chrono::high_resolution_clock::now();
        auto compressed = CompressWithVarint(data);
        std::cout << "Varint compress ratio : " <<  compressed->len_ * 1.0 / data->len_ << std::endl;
        auto decompressed = DecompressVarint(compressed);
        // 获取程序结束时间点
        auto end = std::chrono::high_resolution_clock::now();
        // 计算时间差
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        // 打印程序运行时间
        std::cout << "Time used: " << duration.count() << " us" << std::endl;
        assert(decompressed->len_ == data->len_);
        assert(Validate(data, decompressed));
        delete compressed;
        delete decompressed;
    }
    {
        // 获取程序开始时间点
        auto start = std::chrono::high_resolution_clock::now();
        auto compressed = CompressWithVarint(data);
        std::cout << "Varint compress ratio : " <<  compressed->len_ * 1.0 / data->len_ << std::endl;
        auto compressedZSTD = CompressWithZSTD(compressed);
        std::cout << "ZSTD compress2 ratio : " <<  compressedZSTD->len_ * 1.0 / compressed->len_ << std::endl;
        std::cout << "Total compress ratio : " <<  compressedZSTD->len_ * 1.0 / data->len_ << std::endl;
        auto decompressedZSTD = DecompressZSTD(compressedZSTD);
        assert(decompressedZSTD->len_ == compressed->len_);

        auto decompressed = DecompressVarint(compressed);
        assert(decompressed->len_ == data->len_);
        // 获取程序结束时间点
        auto end = std::chrono::high_resolution_clock::now();
        // 计算时间差
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        // 打印程序运行时间
        std::cout << "Time used: " << duration.count() << " us" << std::endl;
        assert(Validate(data, decompressed));

        delete compressed;
        delete compressedZSTD;
        delete decompressedZSTD;
        delete decompressed;
    }
}