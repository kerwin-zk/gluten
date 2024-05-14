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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "velox/common/time/CpuWallTimer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorStream.h"

#include <arrow/array/util.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/type.h>

#include "memory/VeloxMemoryManager.h"
#include "shuffle/PartitionWriter.h"
#include "shuffle/Partitioner.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/Utils.h"

#include "utils/Print.h"

namespace gluten {

// set 1 to open print
#define VELOX_SHUFFLE_WRITER_PRINT 0

#if VELOX_SHUFFLE_WRITER_PRINT

#define VsPrint Print
#define VsPrintLF PrintLF
#define VsPrintSplit PrintSplit
#define VsPrintSplitLF PrintSplitLF
#define VsPrintVectorRange PrintVectorRange
#define VS_PRINT PRINT
#define VS_PRINTLF PRINTLF
#define VS_PRINT_FUNCTION_NAME PRINT_FUNCTION_NAME
#define VS_PRINT_FUNCTION_SPLIT_LINE PRINT_FUNCTION_SPLIT_LINE
#define VS_PRINT_CONTAINER PRINT_CONTAINER
#define VS_PRINT_CONTAINER_TO_STRING PRINT_CONTAINER_TO_STRING
#define VS_PRINT_CONTAINER_2_STRING PRINT_CONTAINER_2_STRING
#define VS_PRINT_VECTOR_TO_STRING PRINT_VECTOR_TO_STRING
#define VS_PRINT_VECTOR_2_STRING PRINT_VECTOR_2_STRING
#define VS_PRINT_VECTOR_MAPPING PRINT_VECTOR_MAPPING

#else // VELOX_SHUFFLE_WRITER_PRINT

#define VsPrint(...) // NOLINT
#define VsPrintLF(...) // NOLINT
#define VsPrintSplit(...) // NOLINT
#define VsPrintSplitLF(...) // NOLINT
#define VsPrintVectorRange(...) // NOLINT
#define VS_PRINT(a)
#define VS_PRINTLF(a)
#define VS_PRINT_FUNCTION_NAME()
#define VS_PRINT_FUNCTION_SPLIT_LINE()
#define VS_PRINT_CONTAINER(c)
#define VS_PRINT_CONTAINER_TO_STRING(c)
#define VS_PRINT_CONTAINER_2_STRING(c)
#define VS_PRINT_VECTOR_TO_STRING(v)
#define VS_PRINT_VECTOR_2_STRING(v)
#define VS_PRINT_VECTOR_MAPPING(v)

#endif // end of VELOX_SHUFFLE_WRITER_PRINT

enum SplitState { kInit, kPreAlloc, kSplit, kStop };
enum EvictState { kEvictable, kUnevictable };

struct BinaryArrayResizeState {
  bool inResize;
  uint32_t partitionId;
  uint32_t binaryIdx;

  BinaryArrayResizeState() : inResize(false) {}
  BinaryArrayResizeState(uint32_t partitionId, uint32_t binaryIdx)
      : inResize(false), partitionId(partitionId), binaryIdx(binaryIdx) {}
};

class VeloxSortBasedShuffleWriter : public ShuffleWriter {
 public:
  static arrow::Result<std::shared_ptr<VeloxSortBasedShuffleWriter>> create(
      uint32_t numPartitions,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions options,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      arrow::MemoryPool* arrowPool);

  arrow::Status write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) override;

  arrow::Status stop() override;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  arrow::Status evictRowVector(uint32_t partitionId) override;

  arrow::Status evictBatch(
      uint32_t partitionId,
      std::ostringstream* output,
      facebook::velox::OStreamOutputStream* out,
      facebook::velox::RowTypePtr* rowTypePtr);

 private:
  VeloxSortBasedShuffleWriter(
      uint32_t numPartitions,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions options,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      arrow::MemoryPool* pool)
      : ShuffleWriter(numPartitions, std::move(partitionWriter), std::move(options), pool),
        veloxPool_(std::move(veloxPool)) {
    arenas_.resize(numPartitions);
    serdeOptions_.useLosslessTimestamp = true;
  }

  arrow::Status init();

  arrow::Status initFromRowVector(const facebook::velox::RowVector& rv);

  void setSplitState(SplitState state);

  arrow::Status doSort(facebook::velox::RowVectorPtr rv, int64_t memLimit);

  void stat() const;

  SplitState splitState_{kInit};

  EvictState evictState_{kEvictable};

  bool supportAvx512_ = false;

  std::optional<facebook::velox::TypePtr> rowType_;

  std::unique_ptr<facebook::velox::VectorStreamGroup> batch_;

  // Partition ID -> Row Count
  // subscript: Partition ID
  // value: How many rows does this partition have in the current input RowVector
  // Updated for each input RowVector.
  std::vector<uint32_t> partition2RowCount_;

  std::vector<std::unique_ptr<facebook::velox::StreamArena>> arenas_;
  std::unique_ptr<facebook::velox::serializer::presto::PrestoVectorSerde> serde_ =
      std::make_unique<facebook::velox::serializer::presto::PrestoVectorSerde>();

  std::vector<facebook::velox::RowVectorPtr> batches_;

  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;

  std::unordered_map<int32_t, std::vector<int64_t>> rowVectorIndexMap_;

  std::unordered_map<int32_t, std::vector<int64_t>> rowVectorPartitionMap_;

  uint32_t currentInputColumnBytes_ = 0;
  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions serdeOptions_;

  // stat
  enum CpuWallTimingType {
    CpuWallTimingBegin = 0,
    CpuWallTimingCompute = CpuWallTimingBegin,
    CpuWallTimingBuildPartition,
    CpuWallTimingEvictPartition,
    CpuWallTimingHasNull,
    CpuWallTimingCalculateBufferSize,
    CpuWallTimingAllocateBuffer,
    CpuWallTimingCreateRbFromBuffer,
    CpuWallTimingMakeRB,
    CpuWallTimingCacheRB,
    CpuWallTimingFlattenRV,
    CpuWallTimingSplitRV,
    CpuWallTimingIteratePartitions,
    CpuWallTimingStop,
    CpuWallTimingEnd,
    CpuWallTimingNum = CpuWallTimingEnd - CpuWallTimingBegin
  };

  static std::string CpuWallTimingName(CpuWallTimingType type) {
    switch (type) {
      case CpuWallTimingCompute:
        return "CpuWallTimingCompute";
      case CpuWallTimingBuildPartition:
        return "CpuWallTimingBuildPartition";
      case CpuWallTimingEvictPartition:
        return "CpuWallTimingEvictPartition";
      case CpuWallTimingHasNull:
        return "CpuWallTimingHasNull";
      case CpuWallTimingCalculateBufferSize:
        return "CpuWallTimingCalculateBufferSize";
      case CpuWallTimingAllocateBuffer:
        return "CpuWallTimingAllocateBuffer";
      case CpuWallTimingCreateRbFromBuffer:
        return "CpuWallTimingCreateRbFromBuffer";
      case CpuWallTimingMakeRB:
        return "CpuWallTimingMakeRB";
      case CpuWallTimingCacheRB:
        return "CpuWallTimingCacheRB";
      case CpuWallTimingFlattenRV:
        return "CpuWallTimingFlattenRV";
      case CpuWallTimingSplitRV:
        return "CpuWallTimingSplitRV";
      case CpuWallTimingIteratePartitions:
        return "CpuWallTimingIteratePartitions";
      case CpuWallTimingStop:
        return "CpuWallTimingStop";
      default:
        return "CpuWallTimingUnknown";
    }
  }

  facebook::velox::CpuWallTiming cpuWallTimingList_[CpuWallTimingNum];
}; // class VeloxSortBasedShuffleWriter

} // namespace gluten