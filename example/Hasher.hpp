//
// Hash functions.
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

#ifndef LINDORM_TSDB_CONTEST_CPP_HASHER_H
#define LINDORM_TSDB_CONTEST_CPP_HASHER_H

#include "struct/Row.h"
#include "struct/Vin.h"

namespace LindormContest {

    struct VinHasher {
        size_t operator()(const Vin &c) const;
        bool operator()(const Vin &c1, const Vin &c2) const;
    };

    struct RowHasher {
        size_t operator()(const Row &c) const;
        bool operator()(const Row &c1, const Row &c2) const;
    };

    inline size_t VinHasher::operator()(const Vin &c) const {
        // Apple Clang does not support this function, use g++-12 to take place Apple Clang.
        return std::_Hash_impl::hash((void *) c.vin, VIN_LENGTH);
    }

    inline bool VinHasher::operator()(const Vin &c1, const Vin &c2) const {
        return c1 == c2;
    }


    inline size_t RowHasher::operator()(const Row &c) const {
        VinHasher vinHasher;
        return vinHasher(c.vin);
    }

    inline bool RowHasher::operator()(const Row &c1, const Row &c2) const {
        return c1 == c2;
    }
}

#endif //LINDORM_TSDB_CONTEST_CPP_HASHER_H
