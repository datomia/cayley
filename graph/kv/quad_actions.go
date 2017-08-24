package kv

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/cayley/quad"
)

var _ shape.Shape = indexScanShape{}

type indexScan struct {
	Index QuadIndex
	Vals  []uint64
}

func (s *indexScan) IsLookup() bool {
	return len(s.Index.Dirs) == len(s.Vals)
}

type indexScanShape struct {
	Scan indexScan
	Save map[quad.Direction][]string
}

func (s indexScanShape) BuildIterator(qs graph.QuadStore) graph.Iterator {
	kv, ok := qs.(*QuadStore)
	if !ok {
		panic("not a kv")
	}
	return NewQuadIterator(kv, s.Scan.Index, s.Scan.Vals, s.Save)
}
func (s indexScanShape) Optimize(r shape.Optimizer) (shape.Shape, bool) {
	return s, false
}

func (qs *QuadStore) OptimizeShape(s shape.Shape) (shape.Shape, bool) {
	switch s := s.(type) {
	case shape.QuadsAction:
		if len(s.Filter) == 0 {
			return s, false
		}
		// values not covered by indexes will be added as runtime filters
		notCovered := make(map[quad.Direction]uint64, len(s.Filter))
		for k, v := range s.Filter {
			id, ok := v.(Int64Value)
			if !ok {
				return s, false
			}
			notCovered[k] = uint64(id)
		}
		var selected []indexScan
		for _, ind := range qs.getIndexes() {
			// how many directions index covers (index prefix to scan)
			var covers []uint64
			for _, d := range ind.Dirs {
				if v, ok := s.Filter[d]; ok {
					delete(notCovered, d)
					// this is safe, because we checked values on insert to notCovered
					id := uint64(v.(Int64Value))
					if covers == nil {
						covers = make([]uint64, 0, len(ind.Dirs))
					}
					covers = append(covers, id)
				} else {
					// can only scan a prefix
					break
				}
			}
			if len(covers) == 0 {
				continue
			}
			sc := indexScan{
				Index: ind,
				Vals:  covers,
			}
			if sc.IsLookup() && len(covers) == len(s.Filter) {
				// just what we need!
				return shape.NodesFrom{
					Dir: s.Result,
					Quads: indexScanShape{
						Scan: sc,
						Save: s.Save,
					},
				}, true
			}
			selected = append(selected, sc)
		}
		if len(selected) == 0 {
			return s, false
		}
		//filters := make([]linkage, 0, len(notCovered))
		//for dir, id := range notCovered {
		//	filters = append(filters, linkage{dir: dir, val: id})
		//}
		//saves := make([]saveLink, 0, len(s.Save))
		//for dir, tags := range s.Save {
		//	saves = append(saves, saveLink{dir: dir, tags: tags})
		//}
		//// FIXME: instead of doing it all in one iterator, check what parts of this intersection can be simplified to single index scan (and intersect it with others)
		//return quadAction{
		//	Result:  s.Result,
		//	Indexes: selected,
		//	Filters: filters,
		//	Save:    saves,
		//}, true
	}
	return s, false
}
