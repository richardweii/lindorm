//
// Don't modify this file, the evaluation program is compiled
// based on this cpp file.
//

/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "struct/Row.h"

namespace LindormContest {

    Row::Row(Row &&rhs) noexcept : vin(rhs.vin), timestamp(rhs.timestamp), columns(std::move(rhs.columns)) {
    }

    Row::Row(const Row &rhs) = default;

    Row::Row() : vin(), timestamp(0), columns() {
    }

    bool Row::operator==(const Row &rhs) const {
        return vin == rhs.vin &&
               timestamp == rhs.timestamp;
    }

    bool Row::operator!=(const Row &rhs) const {
        return !(rhs == *this);
    }

    bool Row::operator<(const Row &rhs) const {
        int ret = std::memcmp(vin.vin, rhs.vin.vin, VIN_LENGTH);
        if (ret != 0) {
            return ret < 0;
        }
        return timestamp < rhs.timestamp;
    }

    bool Row::operator>(const Row &rhs) const {
        int ret = std::memcmp(vin.vin, rhs.vin.vin, VIN_LENGTH);
        if (ret != 0) {
            return ret > 0;
        }
        return timestamp > rhs.timestamp;
    }

    bool Row::operator<=(const Row &rhs) const {
        return *this < rhs || *this == rhs;
    }

    bool Row::operator>=(const Row &rhs) const {
        return *this > rhs || *this == rhs;
    }

    Row &Row::operator=(const Row &rhs) {
        if (this == &rhs) {
            return *this;
        }
        vin = rhs.vin;
        timestamp = rhs.timestamp;
        columns = rhs.columns;
        return *this;
    }

}
