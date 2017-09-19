// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_EXEC_HDFS_ORC_SCANNER_H
#define IMPALA_EXEC_HDFS_ORC_SCANNER_H

#include "runtime/runtime-state.h"
#include "exec/hdfs-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "orc/orc_proto.pb.h"
#include "orc/OrcFile.hh"
#include "util/runtime-profile-counters.h"

namespace impala {

class CollectionValueBuilder;
struct HdfsFileDesc;

const uint8_t ORC_MAGIC[3] = {'O', 'R', 'C'};

/// This scanner parses ORC files located in HDFS, and writes the content as tuples in
/// the Impala in-memory representation of data, e.g.  (tuples, rows, row batches).
/// For the file format spec, see:
/// https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC
class HdfsOrcScanner : public HdfsScanner {
 public:
  class OrcMemPool : public orc::MemoryPool {
   public:
    OrcMemPool(impala::MemTracker *mem_tracker);
    virtual ~OrcMemPool();

    char* malloc(uint64_t size) override;
    void free(char* p) override;
    MemPool* getMemPool() { return pool; }
   private:
    MemPool *pool;
  };

  class ScanRangeInputStream : public orc::InputStream {
   public:
    ScanRangeInputStream(HdfsOrcScanner *scanner) {
      this->parent_ = scanner;
      if (scanner != NULL) {  // NULL for unit test
        this->filename_ = scanner->filename();
        this->file_desc_ = scanner->scan_node_->GetFileDesc(
            scanner->context_->partition_descriptor()->id(), filename_);
      }
    }

    uint64_t getLength() const {
      return file_desc_->file_length;
    }

    uint64_t getNaturalReadSize() const {
      return parent_->state_->io_mgr()->max_read_buffer_size();
    }

    void read(void *buf, uint64_t length, uint64_t offset);

    const std::string &getName() const {
      return filename_;
    }

  private:
    HdfsOrcScanner *parent_;
    HdfsFileDesc *file_desc_;
    std::string filename_;
  };

  HdfsOrcScanner(HdfsScanNodeBase *scan_node, RuntimeState *state);
  virtual ~HdfsOrcScanner();

  /// Issue just the footer range for each file.  We'll then parse the footer and pick
  /// out the columns we want.
  static Status IssueInitialRanges(HdfsScanNodeBase *scan_node,
                                   const std::vector<HdfsFileDesc *> &files)
                                   WARN_UNUSED_RESULT;

  virtual Status Open(ScannerContext *context) WARN_UNUSED_RESULT;
  virtual Status ProcessSplit() WARN_UNUSED_RESULT;
  virtual void Close(RowBatch* row_batch);

  orc::proto::PostScript getPostscript() { return postscript_; }
  orc::proto::Footer getFooter() { return footer_; }

  Status ParsePostscript(const uint8_t *data, uint64_t size, uint64_t *postscript_len);
  Status ParseFooter(const uint8_t *footer_ptr, uint64_t footer_size);

 private:
  friend class HdfsOrcScannerTest;

  /// Size of the file footer.  This is a guess.  If this value is too little, we will
  /// need to issue another read.
  static const int64_t FOOTER_SIZE;

  /// Index of the current stripe being processed. Initialized to -1 which indicates
  /// that we have not started processing the first stripe yet (GetNext() has not yet
  /// been called).
  int32_t stripe_idx_;

  /// Counts the number of rows processed for the current stripe.
  int64_t stripe_rows_read_;

  /// Indicates whether we should advance to the next stripe in the next GetNext().
  /// Starts out as true to move to the very first row group.
  bool advance_stripe_;

  /// Cached runtime filter contexts, one for each filter that applies to this column.
  vector<const FilterContext *> filter_ctxs_;

  struct LocalFilterStats {
    /// Total number of rows to which each filter was applied
    int64_t considered;

    /// Total number of rows that each filter rejected.
    int64_t rejected;

    /// Total number of rows that each filter could have been applied to (if it were
    /// available from row 0).
    int64_t total_possible;

    /// Use known-width type to act as logical boolean.  Set to 1 if corresponding filter
    /// in filter_ctxs_ should be applied, 0 if it was ineffective and was disabled.
    uint8_t enabled;

    /// Padding to ensure structs do not straddle cache-line boundary.
    uint8_t padding[7];

    LocalFilterStats() : considered(0), rejected(0), total_possible(0), enabled(1) {}
  };

  /// Track statistics of each filter (one for each filter in filter_ctxs_) per scanner so
  /// that expensive aggregation up to the scan node can be performed once, during
  /// Close().
  vector<LocalFilterStats> filter_stats_;

  /// Number of scratch batches processed so far.
  int64_t row_batches_produced_;

  /// Mem pool used in orc readers
  boost::scoped_ptr<OrcMemPool> reader_mem_pool_;

  /// Orc reader will write slot values into this scratch batch for top-level tuples.
  /// See AssembleRows().
  std::unique_ptr<orc::ColumnVectorBatch> scratch_batch_;

  /// File metadata protobuf object
  orc::proto::PostScript postscript_;
  orc::proto::Footer footer_;

  /// Serialized string of orc::proto::FileTail. Used in create orc::reader to avoid
  /// parsing file tail again
  std::string serialized_file_tail_;

  /// The root schema node for this file
  std::unique_ptr<orc::Type> schema_;

  /// ReaderOptions used to create orc reader
  orc::ReaderOptions orc_reader_options_;

  /// Column id is the pre order id in orc::Type tree.
  /// Map from column id to slot descriptor.
  std::map<int, const SlotDescriptor*> col_id_slot_map_;

  /// Scan range for the metadata.
  const DiskIoMgr::ScanRange *metadata_range_;

  /// Timer for materializing rows.  This ignores time getting the next buffer.
  ScopedTimer<MonotonicStopWatch> assemble_rows_timer_;

  /// Average and min/max time spent processing the footer by each split.
  RuntimeProfile::SummaryStatsCounter* process_footer_timer_stats_;

  /// Number of columns that need to be read.
  RuntimeProfile::Counter *num_cols_counter_;

  // TODO
  /// Number of stripes that are skipped because of ORC stripe statistics.
  RuntimeProfile::Counter* num_stats_filtered_stripes_counter_;

  /// Number of stripes that need to be read.
  RuntimeProfile::Counter *num_stripes_counter_;

  /// Number of scanners that end up doing no reads because their splits don't overlap
  /// with the midpoint of any stripe in the file.
  RuntimeProfile::Counter* num_scanners_with_no_reads_counter_;

  const char *filename() const { return metadata_range_->file(); }

  virtual Status GetNextInternal(RowBatch* row_batch) WARN_UNUSED_RESULT;

  /// Check runtime filters' effectiveness every BATCHES_PER_FILTER_SELECTIVITY_CHECK
  /// row batches. Will update 'filter_stats_'.
  void CheckFiltersEffectiveness();

  /// Advances 'stripe_idx_' to the next non-empty stripe and initializes
  /// the column readers to scan it. Recoverable errors are logged to the runtime
  /// state. Only returns a non-OK status if a non-recoverable error is encountered
  /// (or abort_on_error is true). If OK is returned, 'parse_status_' is guaranteed
  /// to be OK as well.
  Status NextStripe() WARN_UNUSED_RESULT;

  /// Reads data using orc-reader to materialize instances of 'tuple_desc'.
  /// Returns a non-OK status if a non-recoverable error was encountered and execution
  /// of this query should be terminated immediately.
  Status AssembleRows(const TupleDescriptor* tuple_desc, RowBatch* row_batch,
      std::unique_ptr<orc::Reader> reader) WARN_UNUSED_RESULT;

  /// Function used by AssembleRows() to read a single row into 'tuple'.
  /// TODO:
  /// Returns false if
  /// execution should be aborted for some reason, otherwise returns true.
  /// materialize_tuple is an in/out parameter. It is set to true by the caller to
  /// materialize the tuple.  If any conjuncts fail, materialize_tuple is set to false
  /// by ReadRow().
  /// 'tuple_materialized' is an output parameter set by this function. If false is
  /// returned, there are no guarantees about 'materialize_tuple' or the state of
  /// column_readers, so execution should be halted immediately.
  /// The template argument IN_COLLECTION allows an optimized version of this code to
  /// be produced in the case when we are materializing the top-level tuple.
  inline void ReadRow(const orc::ColumnVectorBatch &batch, int row_idx,
      const orc::Type* orc_type, Tuple* tuple, RowBatch* dst_batch);

  /// Commit num_rows to the given row batch.
  /// Returns OK if the query is not cancelled and hasn't exceeded any mem limits.
  /// Scanner can call this with 0 rows to flush any pending resources (attached pools
  /// and io buffers) to minimize memory consumption.
  Status CommitRows(RowBatch* dst_batch, int num_rows) WARN_UNUSED_RESULT;

  /// Evaluates 'row' against the i-th runtime filter for this scan node and returns
  /// true if 'row' finds a match in the filter. Returns false otherwise.
  bool EvalRuntimeFilter(int i, TupleRow* row);

  /// Evaluates runtime filters (if any) against the given row. Returns true if
  /// they passed, false otherwise. Maintains the runtime filter stats, determines
  /// whether the filters are effective, and disables them if they are not. This is
  /// replaced by generated code at runtime.
  bool EvalRuntimeFilters(TupleRow* row);

  /// Find and return the last split in the file if it is assigned to this scan node.
  /// Returns NULL otherwise.
  static DiskIoMgr::ScanRange *FindFooterSplit(HdfsFileDesc *file);

  /// Process the file footer and parse file_metadata_.  This should be called with the
  /// last FOOTER_SIZE bytes in context_.
  Status ProcessFileTail();

  /// Save the serialized string of orc::proto::FileTail into serialized_file_tail_.
  /// serialized_file_tail_ will be used in creating orc::reader later.
  void SerializeFileTail(uint64_t postscript_len, uint64_t file_len);

  /// Update reader options used in orc reader by the given tuple descriptor.
  void UpdateReaderOptions(const TupleDescriptor *tuple_desc, OrcMemPool &mem_pool);

  /// Validates the file metadata
  Status ValidateFileMetadata();

  /// Part of the HdfsScanner interface, not used in Orc.
  Status InitNewRange() { return Status::OK(); };

  inline THdfsCompression::type TranslateCompressionKind(::orc::proto::CompressionKind kind);

  /// Unit test constructor
  HdfsOrcScanner();
};

} // namespace impala

#endif
