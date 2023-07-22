//
// Don't modify this file, the evaluation program is compiled
// based on this header file.
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

#ifndef LINDORM_TSDB_CONTEST_CPP_VIN_H
#define LINDORM_TSDB_CONTEST_CPP_VIN_H

#include "Root.h"

namespace LindormContest {
    // The length of vin.
    const int32_t VIN_LENGTH = 17;

    /**
     * The key type of our data schema.
     * One vin is similar to the key of a KV-Engine Database.
     * In our data scheme, one Vin may be related to several rows, where one
     * row corresponds to a specific timestamp of this vin.
     */
    typedef struct Vin {
        char vin[VIN_LENGTH];

        Vin();

        Vin(const Vin &rhs);

        bool operator==(const Vin &rhs) const;

        bool operator!=(const Vin &rhs) const;

        bool operator<(const Vin &rhs) const;

        bool operator>(const Vin &rhs) const;

        bool operator<=(const Vin &rhs) const;

        bool operator>=(const Vin &rhs) const;

        Vin& operator=(const Vin &rhs);
    }Vin;

}

#endif //LINDORM_TSDB_CONTEST_CPP_VIN_H
