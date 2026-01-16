package thunderdb

import (
	"errors"
	"fmt"
	"iter"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/openkvlab/boltdb"
)

type Tx struct {
	tx           *boltdb.Tx
	tempTx       *boltdb.Tx
	tempDb       *boltdb.DB
	tempFilePath string
	maUn         MarshalUnmarshaler
	managed      bool
	stores       map[string]*storage
}

func (tx *Tx) Commit() error {
	if tx.managed {
		panic("cannot commit a managed transaction")
	}
	return tx.tx.Commit()
}

func (tx *Tx) Rollback() error {
	if tx.managed {
		panic("cannot rollback a managed transaction")
	}
	return errors.Join(tx.tx.Rollback(), tx.cleanupTempTx())
}

func (tx *Tx) cleanupTempTx() error {
	if tx.tempTx != nil {
		if err := tx.tempTx.Rollback(); err != nil {
			return err
		}
		tx.tempTx = nil
	}
	if tx.tempDb != nil {
		if err := tx.tempDb.Close(); err != nil {
			return err
		}
		tx.tempDb = nil
	}
	if tx.tempFilePath != "" {
		if err := os.Remove(tx.tempFilePath); err != nil {
			return err
		}
		tx.tempFilePath = ""
	}
	return nil
}

func (tx *Tx) ensureTempTx() (*boltdb.Tx, error) {
	if tx.tempTx != nil {
		return tx.tempTx, nil
	}
	tempFile, err := os.CreateTemp("", "thunder_tempdb_*.db")
	if err != nil {
		return nil, err
	}
	tempFilePath := tempFile.Name()
	tempFile.Close()

	tempDb, err := boltdb.Open(tempFilePath, 0600, &DBOptions{
		NoSync:         true,
		NoGrowSync:     true,
		NoFreelistSync: true,
	})
	if err != nil {
		os.Remove(tempFilePath)
		return nil, err
	}
	tempTx, err := tempDb.Begin(true)
	if err != nil {
		tempDb.Close()
		os.Remove(tempFilePath)
		return nil, err
	}
	tx.tempTx = tempTx
	tx.tempDb = tempDb
	tx.tempFilePath = tempFilePath
	return tempTx, nil
}

func (tx *Tx) ID() int {
	return tx.tx.ID()
}

func (tx *Tx) CreateStorage(
	relation string,
	columnSpecs []ColumnSpec,
	computedColumnSpecs []ComputedColumnSpec,
) error {
	s, err := newStorage(tx.tx, relation, columnSpecs, computedColumnSpecs, tx.maUn)
	if err != nil {
		return err
	}
	tx.stores[relation] = s
	return nil
}

func (tx *Tx) Insert(relation string, value map[int]any) error {
	s, err := tx.loadStorage(relation)
	if err != nil {
		return err
	}
	return s.Insert(value)
}

func (tx *Tx) loadStorage(relation string) (*storage, error) {
	s, ok := tx.stores[relation]
	if ok {
		return s, nil
	}
	s, err := loadStorage(tx.tx, relation, tx.maUn)
	if err != nil {
		return nil, err
	}
	tx.stores[relation] = s
	return s, nil
}

func (tx *Tx) DeleteStorage(relation string) error {
	tnx := tx.tx
	if err := deleteStorage(tnx, relation); err != nil {
		return err
	}
	delete(tx.stores, relation)
	return nil
}

func (tx *Tx) LoadStoredBody(name string) (*StoredBody, error) {
	var metadataObj Metadata
	if err := loadMetadata(tx.tx, name, &metadataObj); err != nil {
		return nil, err
	}
	return &StoredBody{
		storageName: name,
		metadata:    metadataObj,
	}, nil
}

func (tx *Tx) LoadMetadata(relation string) (*Metadata, error) {
	var metadataObj Metadata
	if err := loadMetadata(tx.tx, relation, &metadataObj); err != nil {
		return nil, err
	}
	return &metadataObj, nil
}

func (tx *Tx) Query(body QueryPart, ranges map[int]*BytesRange) (iter.Seq2[*Row, error], error) {
	explored := make(map[bodyFilter]queryNode)
	baseNodes := make([]*backedQueryNode, 0)
	rootNode, err := tx.constructQueryGraph(explored, &baseNodes, body, ranges)
	if err != nil {
		return nil, err
	}
	for _, bn := range baseNodes {
		// fmt.Printf("Propagating from base node %p\n", bn)
		if err := bn.propagateToParents(nil, nil); err != nil {
			return nil, err
		}
	}
	bestIndex := rootNode.metadata().bestIndex(ranges)
	cols := make(map[int]bool, len(rootNode.ColumnSpecs()))
	for i := range rootNode.ColumnSpecs() {
		cols[i] = true
	}
	return rootNode.Find(ranges, cols, bestIndex)
}

func (tx *Tx) constructQueryGraph(explored map[bodyFilter]queryNode, baseNodes *[]*backedQueryNode, body QueryPart, ranges map[int]*BytesRange) (queryNode, error) {
	rangesStr := rangesToString(ranges)
	// fmt.Printf("DEBUG: constructQueryGraph visiting %T (%p) Ranges: %s\n", body, body, rangesStr)
	bf := bodyFilter{
		body:   body,
		filter: rangesStr,
	}
	if node, ok := explored[bf]; ok {
		return node, nil
	}
	switch b := body.(type) {
	case *Head:
		tempTx, err := tx.ensureTempTx()
		if err != nil {
			return nil, err
		}
		backingColumnSpecs := b.metadata.ColumnSpecs
		backingFieldRefs := make([]int, len(b.metadata.ColumnSpecs))
		for i := range backingFieldRefs {
			backingFieldRefs[i] = i
		}
		backingName := fmt.Sprintf("head_backing_%p_%s", b, rangesStr)
		// fmt.Printf("DEBUG: constructQueryGraph Head NEW for %p. BackingName: %s\n", b, backingName)

		backingStorage, err := newStorage(tempTx, backingName, backingColumnSpecs, []ComputedColumnSpec{{
			FieldRefs: backingFieldRefs,
			IsUnique:  true,
		}}, tx.maUn)
		result := &headQueryNode{}
		explored[bf] = result
		children := make([]queryNode, 0, len(b.bodies))
		for _, bbody := range b.bodies {
			childNode, err := tx.constructQueryGraph(explored, baseNodes, bbody, ranges)
			if err != nil {
				return nil, err
			}
			children = append(children, childNode)
		}
		initHeadQueryNode(result, backingStorage, b.ColumnSpecs(), b.ComputedColumnSpecs(), children)
		return result, nil
	case *ProjectedBody:
		result := &projectedQueryNode{}
		explored[bf] = result

		childRanges := make(map[int]*BytesRange)
		for field, r := range ranges {
			if field < len(b.cols) {
				childRanges[b.cols[field]] = r
			} else if field < len(b.cols)+len(b.computedCols) {
				childIndex := b.computedCols[field-len(b.cols)] + len(b.child.ColumnSpecs())
				childRanges[childIndex] = r
			} else {
				return nil, ErrFieldNotFound(fmt.Sprintf("column %d", field))
			}
		}

		childNode, err := tx.constructQueryGraph(explored, baseNodes, b.child, childRanges)
		if err != nil {
			return nil, err
		}
		initProjectedQueryNode(result, childNode, b.cols, b.computedCols)
		return result, nil
	case *JoinedBody:
		result := &joinedQueryNode{}
		explored[bf] = result
		leftRanges, rightRanges := b.splitRanges(ranges)
		leftNode, err := tx.constructQueryGraph(explored, baseNodes, b.left, leftRanges)
		if err != nil {
			return nil, err
		}
		rightNode, err := tx.constructQueryGraph(explored, baseNodes, b.right, rightRanges)
		if err != nil {
			return nil, err
		}
		initJoinedQueryNode(result, leftNode, rightNode, b.conditions)
		return result, nil
	case *StoredBody:
		result := &backedQueryNode{
			ranges: ranges,
		}
		explored[bf] = result
		storage, err := loadStorage(tx.tx, b.storageName, tx.maUn)
		if err != nil {
			return nil, err
		}
		initBackedQueryNode(result, storage, ranges)
		*baseNodes = append(*baseNodes, result)
		return result, nil
	default:
		panic(fmt.Sprintf("unsupported body type: %T", body))
	}
}

type bodyFilter struct {
	body   QueryPart
	filter string
}

func rangesToString(ranges map[int]*BytesRange) string {
	keys := slices.Collect(maps.Keys(ranges))
	slices.Sort(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%d:%s", k, ranges[k].ToString())
	}
	return strings.Join(parts, ";")
}
