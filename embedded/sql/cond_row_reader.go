/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sql

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

type conditionalRowReader struct {
	rowReader RowReader

	condition ValueExp

	// Concurrency
	once     sync.Once
	ctx      context.Context
	cancel   context.CancelFunc
	resultCh chan *readResult

	// Reordering
	nextSeq    uint64
	readBuffer map[uint64]*readResult
}

type readResult struct {
	seq uint64
	row *Row
	err error
}

func newConditionalRowReader(rowReader RowReader, condition ValueExp) *conditionalRowReader {
	return &conditionalRowReader{
		rowReader:  rowReader,
		condition:  condition,
		readBuffer: make(map[uint64]*readResult),
	}
}

func (cr *conditionalRowReader) onClose(callback func()) {
	cr.rowReader.onClose(callback)
}

func (cr *conditionalRowReader) Tx() *SQLTx {
	return cr.rowReader.Tx()
}

func (cr *conditionalRowReader) TableAlias() string {
	return cr.rowReader.TableAlias()
}

func (cr *conditionalRowReader) Parameters() map[string]interface{} {
	return cr.rowReader.Parameters()
}

func (cr *conditionalRowReader) OrderBy() []ColDescriptor {
	return cr.rowReader.OrderBy()
}

func (cr *conditionalRowReader) ScanSpecs() *ScanSpecs {
	return cr.rowReader.ScanSpecs()
}

func (cr *conditionalRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return cr.rowReader.Columns(ctx)
}

func (cr *conditionalRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	return cr.rowReader.colsBySelector(ctx)
}

func (cr *conditionalRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	err := cr.rowReader.InferParameters(ctx, params)
	if err != nil {
		return err
	}

	cols, err := cr.colsBySelector(ctx)
	if err != nil {
		return err
	}

	_, err = cr.condition.inferType(cols, params, cr.TableAlias())

	return err
}

func (cr *conditionalRowReader) start(ctx context.Context) {
	cr.ctx, cr.cancel = context.WithCancel(ctx)

	// Buffer size can be tuned.
	// User mentioned "shared memory that would be streaming".
	// A buffered channel acts as this shared memory buffer.
	const bufferSize = 10000

	inputCh := make(chan *readResult, bufferSize)
	cr.resultCh = make(chan *readResult, bufferSize)

	workerCount := runtime.NumCPU()
	var wg sync.WaitGroup

	// Workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range inputCh {
				if item.err != nil {
					select {
					case cr.resultCh <- item:
					case <-cr.ctx.Done():
					}
					continue
				}

				// Evaluate condition
				// Note: We assume cr.Parameters() and cr.Tx() are safe for concurrent read-only access
				// or that the condition does not modify them/use non-thread-safe features.

				cond, err := cr.condition.substitute(cr.Parameters())
				if err != nil {
					item.err = fmt.Errorf("%w: when evaluating WHERE clause", err)
					select {
					case cr.resultCh <- item:
					case <-cr.ctx.Done():
					}
					continue
				}

				r, err := cond.reduce(cr.Tx(), item.row, cr.TableAlias())
				if err != nil {
					item.err = fmt.Errorf("%w: when evaluating WHERE clause", err)
					select {
					case cr.resultCh <- item:
					case <-cr.ctx.Done():
					}
					continue
				}

				nval, isNull := r.(*NullValue)
				if isNull && nval.Type() == BooleanType {
					// Skip row (effectively filtered out)
					item.row = nil
				} else {
					satisfies, boolExp := r.(*Bool)
					if !boolExp {
						item.err = fmt.Errorf("%w: expected '%s' in WHERE clause, but '%s' was provided", ErrInvalidCondition, BooleanType, r.Type())
					} else if !satisfies.val {
						item.row = nil // Filtered out
					}
				}

				select {
				case cr.resultCh <- item:
				case <-cr.ctx.Done():
				}
			}
		}()
	}

	// Feeder
	go func() {
		defer close(inputCh)
		var seq uint64
		for {
			select {
			case <-cr.ctx.Done():
				return
			default:
			}

			// Read sequentially from underlying reader
			row, err := cr.rowReader.Read(cr.ctx)

			select {
			case inputCh <- &readResult{seq: seq, row: row, err: err}:
			case <-cr.ctx.Done():
				return
			}

			if err != nil {
				return
			}
			seq++
		}
	}()

	// Closer
	go func() {
		wg.Wait()
		close(cr.resultCh)
	}()
}

func (cr *conditionalRowReader) Read(ctx context.Context) (*Row, error) {
	cr.once.Do(func() {
		cr.start(ctx)
	})

	for {
		// Check if we have the next sequence in buffer
		if res, ok := cr.readBuffer[cr.nextSeq]; ok {
			delete(cr.readBuffer, cr.nextSeq)
			cr.nextSeq++
			if res.err != nil {
				return nil, res.err
			}
			if res.row != nil {
				return res.row, nil
			}
			// If row is nil, it was filtered out, loop again
			continue
		}

		// Read from channel
		select {
		case res, ok := <-cr.resultCh:
			if !ok {
				// Channel closed, meaning no more rows or error occurred in feeder
				return nil, ErrNoMoreRows // Default if closed without error
			}

			if res.seq == cr.nextSeq {
				cr.nextSeq++
				if res.err != nil {
					return nil, res.err
				}
				if res.row != nil {
					return res.row, nil
				}
				continue
			} else {
				// Buffer out of order result
				cr.readBuffer[res.seq] = res
			}

		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (cr *conditionalRowReader) Close() error {
	if cr.cancel != nil {
		cr.cancel()
	}
	return cr.rowReader.Close()
}
