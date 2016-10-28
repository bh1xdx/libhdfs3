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

class EncryptedInputStream : public CopyingInputStream {
public:
    EncryptedInputStream(DataTransferProtocol* sender,
            shared_ptr<BufferedSocketReader> reader, int readTimeout): sender(sender), reader(reader),
            readTimeout(readTimeout), readOffset(0) {

    }
    int Read(void * buffer, int size) {
        while (raw.length() == 0 || size > raw.length()) {
            int offset = raw.length();
            raw.resize(8192 + offset);
            if (reader->poll(readTimeout)) {
                int nread = reader->read(&raw[offset], 8192);
                if (nread <= 0)
                    THROW(HdfsIOException, "Couldn't fill buffer")
                raw.resize(nread+offset);
            }
        }
        if (decrypted.length() < raw.length())
            decrypted = sender->unwrap(raw);

        memcpy(buffer, &decrypted[readOffset], size);
        readOffset += size;
        return size;
    }

    int Skip(int count) {
        THROW(HdfsIOException, "skip not implemented")
    }
    std::string & getRaw() {
        return raw;
    }
    std::string getRest() {
        std::string rest;
        rest.resize(decrypted.size());
        memcpy(&rest[0], &decrypted[readOffset], decrypted.size()-readOffset);
        return rest;
    }

    void advance() {
        sender->advanceWrapPosition(raw);
    }
private:
    DataTransferProtocol * sender;
    shared_ptr<BufferedSocketReader> reader;
    std::string raw;
    std::string decrypted;
    int readOffset;
    int readTimeout;
};

DataReader::DataReader(DataTransferProtocol * sender,
        shared_ptr<BufferedSocketReader> reader, int readTimeout) : sender(sender), reader(reader),
            readTimeout(readTimeout), buf(128)
        {
        }

std::vector<char>& DataReader::readPacketHeader(const char* text, int size, int &outsize) {
    int nread = size;
    EncryptedInputStream encrypt(sender, reader, readTimeout);
    shared_ptr<CopyingInputStreamAdaptor> adap = shared_ptr<CopyingInputStreamAdaptor>(new CopyingInputStreamAdaptor((CopyingInputStream*)&encrypt, 1));
    CodedInputStream stream(adap.get());
    bool ret = stream.ReadRaw(&buf[0], nread);
    if (!ret) {
        THROW(HdfsIOException, "cannot parse wrapped datanode data response: %s",
          text);
    }
    rest = encrypt.getRest();
    encrypt.advance();
    buf.resize(nread);
    outsize = nread;
    return buf;
}

std::vector<char>& DataReader::readResponse(const char* text, int &outsize) {
    int size;
    if (sender->isWrapped()) {
        if (!sender->needsLength()) {
            EncryptedInputStream encrypt(sender, reader, readTimeout);
            shared_ptr<CopyingInputStreamAdaptor> adap = shared_ptr<CopyingInputStreamAdaptor>(new CopyingInputStreamAdaptor((CopyingInputStream*)&encrypt, 1));
            CodedInputStream stream(adap.get());
            bool ret = stream.ReadVarint32((uint32*)&size);

            if (!ret) {
                THROW(HdfsIOException, "cannot parse wrapped datanode size response: %s",
                  text);
            }
            ret = stream.ReadRaw(&buf[0], size);
            if (!ret) {
                THROW(HdfsIOException, "cannot parse wrapped datanode data response: %s",
                  text);
            }
            encrypt.advance();
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
        reader->readFully(&buf[0], size, readTimeout);
    }
    buf.resize(size);
    outsize = size;
    return buf;
}


}
}