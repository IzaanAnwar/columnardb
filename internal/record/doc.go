// Package record defines the logical record model used by the datastore.
//
// A record is a logical concept only. There is no physical "row" representation
// stored on disk.
//
// Invariants:
//
//  1. Records are identified solely by their positional index within a segment
//     (0-based). There is no record ID.
//
//  2. All columns within the same segment MUST have identical record_count.
//     Any deviation indicates corruption or a writer bug.
//
//  3. Column values are aligned by position. The value at index i in every column
//     corresponds to the same logical record.
//
//  4. Null values do NOT remove records. A record always exists at a given index,
//     even if all columns contain NULL at that position.
//
//  5. Readers and writers MUST NOT assume row-based storage. Rows are materialized
//     only at query time.
//
// These invariants are assumed throughout the storage engine. Violating them
// results in undefined behavior.
package record
