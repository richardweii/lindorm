//
// Sample showing the usages of ColumnValue.
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

#include "struct/ColumnValue.h"

using namespace LindormContest;
using namespace std;

int main() {
    // Integer value read and write.
    int i;
    ColumnValue intC1(128);
    cout << intC1.getIntegerValue(i) << " " << i << endl;
    ColumnValue intC2(0);
    cout << intC2.getIntegerValue(i) << " " << i << endl;

    // Double Float value read and write.
    double d;
    ColumnValue doubleC1(128.234);
    cout << doubleC1.getDoubleFloatValue(d) << " " << d << endl;
    ColumnValue doubleC2(0.0);
    cout << doubleC2.getDoubleFloatValue(d) << " " << d << endl;

    // String value from char*.
    pair<int32_t, const char *> sPair1;
    ColumnValue strC1("str1", 4);
    char retS1[5];
    cout << strC1.getStringValue(sPair1) << " ";
    memcpy(retS1, sPair1.second, 4);
    retS1[4] = '\0';
    cout << sPair1.first << " " << retS1 << endl;
    pair<int32_t, const char *> sPair2;
    ColumnValue strC2("", 0);
    cout << strC2.getStringValue(sPair2) << " " << sPair2.first << endl;

    // String value from std::string.
    ColumnValue strC3("str3-");
    pair<int32_t, const char *> sPair3;
    char retS3[6];
    retS3[5] = '\0';
    cout << strC3.getStringValue(sPair3) << " ";
    memcpy(retS3, sPair3.second, 5);
    cout << sPair3.first << " " << retS3 << endl;
    ColumnValue strC4("");
    pair<int32_t, const char *> sPair4;
    cout << strC4.getStringValue(sPair4) << " " << sPair4.first << endl;

    // String value from std::vector.
    vector<char> vectorS5;
    vectorS5.reserve(2);
    vectorS5.push_back('a');
    vectorS5.push_back('b');
    ColumnValue strC5(vectorS5);
    char retS5[3];
    retS5[2] = '\0';
    pair<int32_t, const char *> sPair5;
    cout << strC5.getStringValue(sPair5) << " ";
    memcpy(retS5, sPair5.second, 2);
    cout << sPair5.first << " " << retS5 << endl;
    vector<char> vectorS6;
    vectorS6.reserve(15);
    ColumnValue strC6(vectorS6);
    pair<int32_t, const char *> sPair6;
    cout << strC6.getStringValue(sPair6) << " " << sPair6.first << endl;

    // Copy constructor for integer type.
    ColumnValue rhs1(intC1);
    cout << rhs1.getIntegerValue(i) << " " << i << endl;
    ColumnValue rhs2(intC2);
    cout << rhs2.getIntegerValue(i) << " " << i << endl;

    // Copy constructor for double type.
    ColumnValue rhs3(doubleC1);
    cout << rhs3.getDoubleFloatValue(d) << " " << d << endl;
    ColumnValue rhs4(doubleC2);
    cout << rhs4.getDoubleFloatValue(d) << " " << d << endl;

    // Copy constructor for string type.
    ColumnValue rhs5(strC5);
    pair<int32_t, const char *> sPair7;
    char retS7[3];
    retS7[2] = '\0';
    cout << rhs5.getStringValue(sPair7) << " ";
    memcpy(retS7, sPair7.second, 2);
    cout << sPair7.first << " " << retS7 << endl;
    ColumnValue rhs6(strC6);
    pair<int32_t, const char *> sPair8;
    cout << rhs6.getStringValue(sPair8) << " " << sPair8.first << endl;

    // Move constructor.
    ColumnValue moveCRhs(strC5);
    ColumnValue moveCLhs(std::move(moveCRhs));
    cout << getNameFromColumnType(moveCRhs.columnType) << " " << (moveCRhs.columnData == nullptr) << endl;
    cout << getNameFromColumnType(moveCLhs.columnType) << " "
         << (moveCLhs.columnData[0] == strC5.columnData[0]) << " "
         << (moveCLhs.columnData[5] == strC5.columnData[5]) << endl;

    // Get raw data size.
    cout << intC1.getRawDataSize() << endl;
    cout << intC2.getRawDataSize() << endl;
    cout << doubleC1.getRawDataSize() << endl;
    cout << doubleC2.getRawDataSize() << endl;
    cout << strC5.getRawDataSize() << endl;
    cout << strC6.getRawDataSize() << endl;

    // Operator ==.
    ColumnValue equalsInt(intC1);
    ColumnValue equalsDouble(doubleC1);
    ColumnValue nonEqualsInt(23);
    ColumnValue nonEqualsDouble(35.33);
    ColumnValue equalsStrC5(strC5);
    ColumnValue equalsStrC6(strC6);
    ColumnValue nonEqualsStr("aaaaaa");
    cout << (equalsInt == intC1) << " " << (nonEqualsInt == intC1) << endl;
    cout << (equalsDouble == doubleC1) << " " << (nonEqualsDouble == doubleC1) << endl;
    cout << (equalsInt == doubleC1) << " " << (equalsDouble == equalsStrC6) << endl;
    cout << (equalsStrC5 == doubleC1) << " " << (equalsStrC6 == intC1) << endl;
    cout << (equalsStrC5 == strC5) << " " << (equalsStrC5 ==  nonEqualsStr) << endl;
    cout << (equalsStrC6 == strC6) << " " << (equalsStrC6 ==  nonEqualsStr) << endl;

    // Operator =.
    ColumnValue intLhsForEO(15);
    ColumnValue doubleLhsForE0(15.22);
    ColumnValue strLhsForE0("FSD");
    intLhsForEO = strC1;
    doubleLhsForE0 = intC1;
    strLhsForE0 = doubleC1;
    cout << (intLhsForEO == strC1) << endl;
    cout << (doubleLhsForE0 == intC1) << endl;
    cout << (strLhsForE0 == doubleC1) << endl;

    return 0;
}