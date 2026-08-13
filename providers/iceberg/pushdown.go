package iceberg

import (
	"context"
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/table"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// ScanWithOptions starts a scan that honors the projection exactly and
// hands the pushed filters and limit hint to iceberg-go's own scan
// machinery: one converted row-filter expression drives manifest pruning,
// per-data-file metrics pruning, in-file row-group/bloom pruning, and
// row-level filtering, all library-side (design D6). Filters that cannot
// be converted faithfully are dropped whole — the engine re-applies every
// pushed filter, so dropping only widens the scan and never changes
// results.
func (t *tableProvider) ScanWithOptions(ctx context.Context, opts *datafusion.ScanOptions) (array.RecordReader, error) {
	if opts == nil {
		return t.Scan(ctx)
	}

	tbl, err := t.load(ctx)
	if err != nil {
		return nil, err
	}

	// The registered Arrow field names are the Iceberg schema names
	// verbatim, so matching is always case-sensitive; case-insensitivity
	// could only introduce ambiguity.
	scanOpts := []table.ScanOption{table.WithCaseSensitive(true)}

	if filter := convertFilters(opts.Filters, tbl.Schema()); filter != nil {
		scanOpts = append(scanOpts, table.WithRowFilter(filter))
	}
	if opts.Limit >= 0 {
		scanOpts = append(scanOpts, table.WithLimit(opts.Limit))
	}

	// Projection: WithSelectedFields prunes what is read, but the output
	// arrives in table-schema order; a ProjectReader permutation restores
	// the requested order when they differ (design D7).
	var perm []int
	if opts.Projection != nil {
		names, p, err := t.projectionPlan(opts.Projection)
		if err != nil {
			return nil, err
		}
		scanOpts = append(scanOpts, table.WithSelectedFields(names...))
		perm = p
	}

	scan := tbl.Scan(scanOpts...)
	schema, itr, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("iceberg: scan %s: %w", t.describe(), err)
	}
	reader := readerFromSeq(schema, itr)
	if perm != nil {
		reader = datafusion.ProjectReader(reader, perm)
	}
	return reader, nil
}

// projectionPlan maps the projected field indices to the field names to
// select and, when the scan's schema-ordered output differs from the
// requested order, the permutation that restores it. An empty projection
// selects the first schema field and projects it away, yielding
// zero-column batches with correct row counts.
func (t *tableProvider) projectionPlan(projection []int) (names []string, perm []int, err error) {
	if len(projection) == 0 {
		return []string{t.schema.Field(0).Name}, []int{}, nil
	}

	unique := make([]int, 0, len(projection))
	seen := make(map[int]bool, len(projection))
	for _, idx := range projection {
		if idx < 0 || idx >= t.schema.NumFields() {
			return nil, nil, fmt.Errorf("iceberg: projection index %d out of range for %s", idx, t.describe())
		}
		if !seen[idx] {
			seen[idx] = true
			unique = append(unique, idx)
		}
	}

	// The scan returns the selected fields in table-schema order.
	outputOrder := append([]int(nil), unique...)
	sort.Ints(outputOrder)
	posInOutput := make(map[int]int, len(outputOrder))
	names = make([]string, len(outputOrder))
	for pos, idx := range outputOrder {
		posInOutput[idx] = pos
		names[pos] = t.schema.Field(idx).Name
	}

	identity := len(projection) == len(outputOrder)
	perm = make([]int, len(projection))
	for i, idx := range projection {
		perm[i] = posInOutput[idx]
		if perm[i] != i {
			identity = false
		}
	}
	if identity {
		return names, nil, nil
	}
	return names, perm, nil
}
