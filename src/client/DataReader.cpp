/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "DateTime.h"
#include "Pipeline.h"
#include "Logger.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "datatransfer.pb.h"
#include "DataReader.h"


#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

using namespace ::google::protobuf;
using namespace google::protobuf::io;

namespace Hdfs {
namespace Internal {


int fillData(BufferedSocketReader *reader, std::string &raw) {
    int offset=0;
    int numRetries=0;
    raw.resize(65536);
    while (numRetries < 5 && offset < 65536) {
        if (reader->poll(100)) {
            int nread = 0;

            try {
                nread = reader->read(&raw[offset], 65536-offset);
            }
            catch (HdfsEndOfStream ex) {
                if (offset == 0)
                    raise;
                break;
            }
            if (nread) {
                offset += nread;
                numRetries = 0;
            } else {
                numRetries += 1;
            }
        } else {
            numRetries += 1;
        }
    }
    if (offset == 0) {
        THROW(HdfsIOException, "Couldn't fill buffer")
    }
    raw.resize(offset);
    return offset;

}
DataReader::DataReader(DataTransferProtocol * sender,
        shared_ptr<BufferedSocketReader> reader, int readTimeout) : sender(sender), reader(reader),
            readTimeout(readTimeout), buf(128)
        {
            // max size of packet
            raw.resize(65536);
            decrypted.resize(65536);
        }

std::vector<char>& DataReader::readPacketHeader(const char* text, int size, int &outsize) {
    int nread = size;
    if (rest.size()) {
        decrypted = rest;
        rest = "";
        if (decrypted.size() < size) {
            fillData(reader.get(), raw);
            decrypted += sender->unwrap(raw);
        }
    } else {
        fillData(reader.get(), raw);
        decrypted = sender->unwrap(raw);
    }
    CodedInputStream stream(reinterpret_cast<const uint8_t *>(decrypted.c_str()), decrypted.length());
    buf.resize(nread);
    bool ret = stream.ReadRaw(&buf[0], nread);
    if (!ret) {
        THROW(HdfsIOException, "cannot parse wrapped datanode data response: %s",
          text);
    }
    rest.assign(&decrypted[nread], decrypted.size()-nread);
    outsize = nread;
    return buf;
}

void DataReader::getMissing(int size) {
    while (size > rest.size()) {
        fillData(reader.get(), raw);
        decrypted = sender->unwrap(raw);
        rest = rest + decrypted;
    }
}

void DataReader::reduceRest(int size) {
    std::string temp;
    temp.assign(rest.c_str() + size, rest.size()-size);
    rest = temp;
}

std::vector<char>& DataReader::readResponse(const char* text, int &outsize) {
    int size;
    if (sender->isWrapped()) {
        if (!sender->needsLength()) {
            if (rest.size()) {
                decrypted = rest;
                rest = "";
            } else {
                fillData(reader.get(), raw);
                decrypted = sender->unwrap(raw);
            }
            CodedInputStream stream(reinterpret_cast<const uint8_t *>(decrypted.c_str()), decrypted.length());
            bool ret = stream.ReadVarint32((uint32*)&size);

            if (!ret) {
                THROW(HdfsIOException, "cannot parse wrapped datanode size response: %s",
                  text);
            }
            if (decrypted.size() < size) {
                fillData(reader.get(), raw);
                decrypted += sender->unwrap(raw);
            }
            buf.resize(size);
            ret = stream.ReadRaw(&buf[0], size);
            if (!ret) {
                THROW(HdfsIOException, "cannot parse wrapped datanode data response: %s",
                  text);
            }
            int offset;
            int pos = decrypted.find(&buf[0], 0, size);
            if (pos == string::npos) {
                THROW(HdfsIOException, "cannot parse wrapped datanode data response: %s",
                  text);
            }

            rest.assign(&decrypted[size+pos], decrypted.size()-(size+pos));
        } else {
            size = reader->readBigEndianInt32(readTimeout);
            reader->readFully(&buf[0], size, readTimeout);

            std::string data = sender->unwrap(std::string(buf.begin(), buf.end()));

            bool ret;
            CodedInputStream stream(reinterpret_cast<const uint8_t *>(data.c_str()), data.length());
            ret = stream.ReadVarint32((uint32*)&size);
            if (!ret) {
                THROW(HdfsIOException, "cannot parse wrapped datanode size response: %s",
                  text);
            }
            buf.resize(size);
            ret = stream.ReadRaw(&buf[0], size);
            if (!ret) {
                THROW(HdfsIOException, "cannot parse wrapped datanode data response: %s",
                  text);
            }
       }
    }
    else {
        size = reader->readVarint32(readTimeout);
        buf.resize(size);
        reader->readFully(&buf[0], size, readTimeout);
    }
    outsize = size;
    return buf;
}


}
}