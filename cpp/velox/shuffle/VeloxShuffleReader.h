/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <velox/common/memory/ByteStream.h>
#include <velox/serializers/PrestoSerializer.h>

#include "shuffle/ShuffleReader.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

class VeloxShuffleReader final : public ShuffleReader {
 public:
  VeloxShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ShuffleReaderOptions options,
      arrow::MemoryPool* pool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool);

  std::shared_ptr<ResultIterator> readStream(std::shared_ptr<arrow::io::InputStream> in) override;

  std::shared_ptr<ResultIterator> readStream(std::shared_ptr<JavaInputStreamWrapper> in) override;

  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions serdeOptions_;

 private:
  facebook::velox::RowTypePtr rowType_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
};

extern bool veloxShuffleReaderPrintFlag;

class VeloxInputStream : public facebook::velox::ByteInputStream {
public:
  VeloxInputStream(std::shared_ptr<JavaInputStreamWrapper> input, facebook::velox::BufferPtr buffer)
      : input_(std::move(input)),
        buffer_(std::move(buffer)) {
    next(true);
  }

  bool atEnd() const override {
    return offset_ == 0 && ranges()[0].position >= ranges()[0].size;
  }

private:
  void next(bool throwIfPastEnd) override {
    const int32_t readBytes = buffer_->capacity();
    VELOX_CHECK_LT(0, readBytes, "Reading past end of spill file");
    setRange({buffer_->asMutable<uint8_t>(), readBytes, 0});
    offset_ = input_->read(readBytes, buffer_->asMutable<char>());
  }

  const std::shared_ptr<JavaInputStreamWrapper> input_;
  const facebook::velox::BufferPtr buffer_;
  uint64_t offset_ = -1;
};
} // namespace gluten
