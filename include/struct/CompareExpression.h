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

#ifndef LINDORM_TSDB_CONTEST_CPP_COMPAREEXPRESSION_H
#define LINDORM_TSDB_CONTEST_CPP_COMPAREEXPRESSION_H

#include "ColumnValue.h"

namespace LindormContest {

    /**
     * An enum stands for the column filter operator.
     * For the sake of simplicity, we only support EQUAL and GREATER for this contest.
     */
    enum CompareOp {
        EQUAL = 0,
        GREATER = 1
    };

    /**
     * A class for the expression of a column compare operation.
     */
    typedef struct CompareExpression {
        ColumnValue value;
        CompareOp compareOp;
        bool doCompare(const ColumnValue &value1) const;
    }CompareExpression;
}



#endif //LINDORM_TSDB_CONTEST_CPP_COMPAREEXPRESSION_H
