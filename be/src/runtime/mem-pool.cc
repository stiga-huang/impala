// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/mem-pool.h"

#include <sstream>

#include "common/names.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "util/bit-util.h"

using namespace impala;

#define MEM_POOL_POISON (0x66aa77bb)

const int MemPool::INITIAL_CHUNK_SIZE;
const int MemPool::MAX_CHUNK_SIZE;

const char* MemPool::LLVM_CLASS_NAME = "class.impala::MemPool";
const int MemPool::DEFAULT_ALIGNMENT;
uint32_t MemPool::zero_length_region_ alignas(std::max_align_t) = MEM_POOL_POISON;

MemPool::MemPool(MemTracker* mem_tracker, bool enforce_binary_chunk_sizes)
  : current_chunk_idx_(-1),
    next_chunk_size_(INITIAL_CHUNK_SIZE),
    total_allocated_bytes_(0),
    total_reserved_bytes_(0),
    total_reserved_bytes_from_malloc_(0),
    mem_tracker_(mem_tracker),
    enforce_binary_chunk_sizes_(enforce_binary_chunk_sizes),
    backed_by_buffer_pool_(false) {
  DCHECK(mem_tracker != NULL);
  DCHECK_EQ(zero_length_region_, MEM_POOL_POISON);
}

MemPool::MemPool(BufferPool::ClientHandle* client)
  : current_chunk_idx_(-1),
    next_chunk_size_(INITIAL_CHUNK_SIZE),
    total_allocated_bytes_(0),
    total_reserved_bytes_(0),
    total_reserved_bytes_from_malloc_(0),
    mem_tracker_(nullptr),
    enforce_binary_chunk_sizes_(false),
    backed_by_buffer_pool_(true),
    bp_client_(client) {
}

MemPool::ChunkInfo::ChunkInfo(int64_t size, uint8_t* buf)
  : data(buf),
    size(size),
    allocated_bytes(0) {
}

MemPool::~MemPool() {
  for (auto& chunk : chunks_) {
    if (chunk.buffer_idx < 0) free(chunk.data);
  }
  // No need to update counters since chunks_ is expected to be empty here.
  DCHECK(chunks_.empty()) << "Must call FreeAll() or AcquireData() for this pool";
  DCHECK_EQ(zero_length_region_, MEM_POOL_POISON);
}

void MemPool::Clear() {
  DFAKE_SCOPED_LOCK(mutex_);
  current_chunk_idx_ = -1;
  for (auto& chunk: chunks_) {
    chunk.allocated_bytes = 0;
    ASAN_POISON_MEMORY_REGION(chunk.data, chunk.size);
  }
  total_allocated_bytes_ = 0;
  DCHECK(CheckIntegrity(false));
}

void MemPool::FreeAll() {
  DFAKE_SCOPED_LOCK(mutex_);
  int64_t total_bytes_released = 0;
  MonotonicStopWatch sw;
  sw.Start();
  for (auto& chunk: chunks_) {
    if (chunk.buffer_idx >= 0) {
      DCHECK_LT(chunk.buffer_idx, buffers_.size());
      ExecEnv::GetInstance()->buffer_pool()->FreeBuffer(
          buffers_[chunk.buffer_idx].client, &buffers_[chunk.buffer_idx].buffer);
    } else {
      total_bytes_released += chunk.size;
      free(chunk.data);
    }
    counters_.sys_free_duration.UpdateCounter(sw.Reset());
    counters_.freed_bytes.UpdateCounter(chunk.size);
  }
  for (const auto& b : buffers_) {
    DCHECK(!b.buffer.is_open()) << "dangling buffer " << b.buffer.DebugString();
  }
  buffers_.clear();
  chunks_.clear();
  next_chunk_size_ = INITIAL_CHUNK_SIZE;
  current_chunk_idx_ = -1;
  total_allocated_bytes_ = 0;
  total_reserved_bytes_ = 0;

  if (mem_tracker_) mem_tracker_->Release(total_bytes_released);
}

bool MemPool::FindChunk(int64_t min_size, bool check_limits) noexcept {
  // Try to allocate from a free chunk. We may have free chunks after the current chunk
  // if Clear() was called. The current chunk may be free if ReturnPartialAllocation()
  // was called. The first free chunk (if there is one) can therefore be either the
  // current chunk or the chunk immediately after the current chunk.
  int first_free_idx;
  if (current_chunk_idx_ == -1) {
    first_free_idx = 0;
  } else {
    DCHECK_GE(current_chunk_idx_, 0);
    first_free_idx = current_chunk_idx_ +
        (chunks_[current_chunk_idx_].allocated_bytes > 0);
  }
  for (int idx = current_chunk_idx_ + 1; idx < chunks_.size(); ++idx) {
    // All chunks after 'current_chunk_idx_' should be free.
    DCHECK_EQ(chunks_[idx].allocated_bytes, 0);
    if (chunks_[idx].size >= min_size) {
      // This chunk is big enough. Move it before the other free chunks.
      if (idx != first_free_idx) std::swap(chunks_[idx], chunks_[first_free_idx]);
      current_chunk_idx_ = first_free_idx;
      DCHECK(CheckIntegrity(true));
      return true;
    }
  }

  // Didn't find a big enough free chunk - need to allocate new chunk.
  int64_t chunk_size;
  DCHECK_LE(next_chunk_size_, MAX_CHUNK_SIZE);
  DCHECK_GE(next_chunk_size_, INITIAL_CHUNK_SIZE);
  chunk_size = max<int64_t>(min_size, next_chunk_size_);
  MonotonicStopWatch alloc_sw;
  if (backed_by_buffer_pool_) {
    BufferPool* buffer_pool = ExecEnv::GetInstance()->buffer_pool();
    BufferInfo buffer_info;
    buffer_info.client = bp_client_;
    chunk_size = BitUtil::RoundUpToPowerOfTwo(chunk_size);
    chunk_size = max(buffer_pool->min_buffer_len(), chunk_size);
    alloc_sw.Start();
    Status s = buffer_pool->AllocateUnreservedBuffer(
        bp_client_, chunk_size, &buffer_info.buffer);
    if (UNLIKELY(!s.ok() || !buffer_info.buffer.is_open())) return false;
    InsertChunk(first_free_idx, chunk_size, buffer_info.buffer.data());
    buffers_.push_back(std::move(buffer_info));
    chunks_.back().buffer_idx = buffers_.size() - 1;
    counters_.sys_alloc_duration.UpdateCounter(alloc_sw.ElapsedTime());
    counters_.allocated_bytes.UpdateCounter(chunk_size);
    return true;
  }
  // TODO: we need to allow this to acquire buffers from other MemPools.
  DCHECK_EQ(bp_client_, nullptr);
  if (enforce_binary_chunk_sizes_) chunk_size = BitUtil::RoundUpToPowerOfTwo(chunk_size);
  if (check_limits) {
    if (!mem_tracker_->TryConsume(chunk_size)) return false;
  } else {
    mem_tracker_->Consume(chunk_size);
  }

  alloc_sw.Start();
  // Allocate a new chunk. Return early if malloc fails.
  uint8_t* buf = reinterpret_cast<uint8_t*>(malloc(chunk_size));
  uint64_t duration = alloc_sw.ElapsedTime();
  counters_.sys_alloc_duration.UpdateCounter(duration);
  if (UNLIKELY(buf == NULL)) {
    mem_tracker_->Release(chunk_size);
    return false;
  }
  counters_.allocated_bytes.UpdateCounter(chunk_size);
  ASAN_POISON_MEMORY_REGION(buf, chunk_size);
  total_reserved_bytes_from_malloc_ += chunk_size;
  InsertChunk(first_free_idx, chunk_size, buf);
  return true;
}

void MemPool::InsertChunk(int first_free_idx, int64_t chunk_size, uint8_t* buf) {
  // Put it before the first free chunk. If no free chunks, it goes at the end.
  if (first_free_idx == static_cast<int>(chunks_.size())) {
    chunks_.push_back(ChunkInfo(chunk_size, buf));
  } else {
    chunks_.insert(chunks_.begin() + first_free_idx, ChunkInfo(chunk_size, buf));
  }
  current_chunk_idx_ = first_free_idx;
  total_reserved_bytes_ += chunk_size;
  // Don't increment the chunk size until the allocation succeeds: if an attempted
  // large allocation fails we don't want to increase the chunk size further.
  next_chunk_size_ = static_cast<int>(min<int64_t>(chunk_size * 2, MAX_CHUNK_SIZE));

  DCHECK(CheckIntegrity(true));
}

void MemPool::AcquireData(MemPool* src, bool keep_current) {
  DFAKE_SCOPED_LOCK(mutex_);
  DCHECK(src->CheckIntegrity(false));
  int num_acquired_chunks;
  if (keep_current) {
    num_acquired_chunks = src->current_chunk_idx_;
  } else if (src->GetFreeOffset() == 0) {
    // nothing in the last chunk
    num_acquired_chunks = src->current_chunk_idx_;
  } else {
    num_acquired_chunks = src->current_chunk_idx_ + 1;
  }

  if (num_acquired_chunks <= 0) {
    if (!keep_current) src->FreeAll();
    return;
  }

  auto src_end_chunk = src->chunks_.begin() + num_acquired_chunks;
  int64_t total_transferred_bytes = 0;
  int64_t total_transferred_bytes_from_malloc = 0;
  for (auto i = src->chunks_.begin(); i != src_end_chunk; ++i) {
    total_transferred_bytes += i->size;
    if (i->buffer_idx < 0) total_transferred_bytes_from_malloc += i->size;
  }
  src->total_reserved_bytes_ -= total_transferred_bytes;
  total_reserved_bytes_ += total_transferred_bytes;
  src->total_reserved_bytes_from_malloc_ -= total_transferred_bytes_from_malloc;
  total_reserved_bytes_from_malloc_ += total_transferred_bytes_from_malloc;

  if (total_transferred_bytes_from_malloc > 0) {
    DCHECK(src->mem_tracker_ && mem_tracker_);
    src->mem_tracker_->TransferTo(mem_tracker_, total_transferred_bytes_from_malloc);
  }

  // insert new chunks after current_chunk_idx_. We must calculate current_chunk_idx_ + 1
  // before finding the offset from chunks_.begin() because current_chunk_idx_ can be -1
  // and adding a negative number to chunks_.begin() is undefined behavior in C++.
  auto insert_chunk = chunks_.begin() + (current_chunk_idx_ + 1);
  // Move BufferPool buffers referenced by the chunks that we move.
  int num_moved_buffers = 0;
  for (auto it = src->chunks_.begin(); it != src_end_chunk; ++insert_chunk, ++it) {
    insert_chunk = chunks_.insert(insert_chunk, *it);
    if (it->buffer_idx >= 0) {
      DCHECK_LT(it->buffer_idx, src->buffers_.size());
      // TODO: Use BufferPool::TransferBuffer(). Who should own the RowBatch?
      buffers_.push_back(std::move(src->buffers_[it->buffer_idx]));
      insert_chunk->buffer_idx = buffers_.size() - 1;
      ++num_moved_buffers;
    }
  }
  src->chunks_.erase(src->chunks_.begin(), src_end_chunk);
  // Erase closed buffers after moving
  // TODO: use erase_if once we upgrade to C++20
  for (auto it = src->buffers_.begin(); it != src->buffers_.end(); ) {
    if (it->buffer.is_open()) {
      ++it;
    } else {
      src->buffers_.erase(it);
      --num_moved_buffers;
    }
  }
  DCHECK_EQ(num_moved_buffers, 0);
  current_chunk_idx_ += num_acquired_chunks;

  if (keep_current) {
    src->current_chunk_idx_ = 0;
    DCHECK(src->chunks_.size() == 1 || src->chunks_[1].allocated_bytes == 0);
    total_allocated_bytes_ += src->total_allocated_bytes_ - src->GetFreeOffset();
    src->total_allocated_bytes_ = src->GetFreeOffset();
  } else {
    src->current_chunk_idx_ = -1;
    total_allocated_bytes_ += src->total_allocated_bytes_;
    src->total_allocated_bytes_ = 0;
  }

  if (!keep_current) src->FreeAll();
  DCHECK(src->CheckIntegrity(false));
  DCHECK(CheckIntegrity(false));
}

void MemPool::SetMemTracker(MemTracker* new_tracker) {
  DFAKE_SCOPED_LOCK(mutex_);
  if (mem_tracker_) {
    mem_tracker_->TransferTo(new_tracker, total_reserved_bytes_from_malloc_);
  }
  mem_tracker_ = new_tracker;
}

string MemPool::DebugString() {
  stringstream out;
  char str[16];
  out << "MemPool " << (void*)this << " (#chunks=" << chunks_.size() << " [";
  for (int i = 0; i < chunks_.size(); ++i) {
    sprintf(str, "0x%lx=", reinterpret_cast<size_t>(chunks_[i].data));
    out << (i > 0 ? " " : "")
        << str
        << chunks_[i].size
        << "/" << chunks_[i].allocated_bytes
        << " bid=" << chunks_[i].buffer_idx;
  }
  out << "] current_chunk=" << current_chunk_idx_
      << " total_sizes=" << GetTotalChunkSizes()
      << " total_alloc=" << total_allocated_bytes_
      << " total_buffers=" << buffers_.size()
      << ")";
  for (auto& buffer : buffers_) {
    out << "\n" << buffer.buffer.DebugString();
  }
  return out.str();
}

int64_t MemPool::GetTotalChunkSizes() const {
  DFAKE_SCOPED_LOCK(mutex_);
  int64_t result = 0;
  for (int i = 0; i < chunks_.size(); ++i) {
    result += chunks_[i].size;
  }
  return result;
}

bool MemPool::CheckIntegrity(bool check_current_chunk_empty) {
  DCHECK_EQ(zero_length_region_, MEM_POOL_POISON);
  DCHECK_LT(current_chunk_idx_, static_cast<int>(chunks_.size()));

  // check that current_chunk_idx_ points to the last chunk with allocated data
  int64_t total_allocated = 0;
  int64_t total_reserved = 0;
  for (int i = 0; i < chunks_.size(); ++i) {
    DCHECK_GT(chunks_[i].size, 0);
    total_reserved += chunks_[i].size;
    if (i < current_chunk_idx_) {
      DCHECK_GT(chunks_[i].allocated_bytes, 0);
    } else if (i == current_chunk_idx_) {
      DCHECK_GE(chunks_[i].allocated_bytes, 0);
      if (check_current_chunk_empty) DCHECK_EQ(chunks_[i].allocated_bytes, 0);
    } else {
      DCHECK_EQ(chunks_[i].allocated_bytes, 0);
    }
    total_allocated += chunks_[i].allocated_bytes;
  }
  DCHECK_EQ(total_allocated, total_allocated_bytes_);
  DCHECK_EQ(total_reserved, total_reserved_bytes_);
  return true;
}

int64_t MemPool::GetUsedReservation() const {
  int64_t reservation = 0;
  for (const auto& b : buffers_) {
    if (b.buffer.is_open()) reservation += b.buffer.len();
  }
  return reservation;
}
