package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dbtrail/dbtrail/internal/byos"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// StartCapture runs the real capture (streamrun.One) from the source behind
// sourceDSN into the index behind indexDSN, schema only, and waits for its
// first checkpoint: with none saved, One attaches at the source's CURRENT
// position, so writes before that checkpoint would never be streamed. The
// returned channel is where One's result lands (buffered: a run watches it
// so a capture that dies mid-run is reported as that); stop cancels the
// capture and returns any result that is not the cancel it asked for, or
// an error when it did not stop within the wait.
func StartCapture(ctx context.Context, indexDB *sql.DB, indexDSN, sourceDSN, schema string, serverID uint32) (errCh chan error, stop func() error, err error) {
	cctx, cancel := context.WithCancel(ctx)
	errCh = make(chan error, 1)
	go func() {
		errCh <- streamrun.One(cctx, streamrun.Config{
			IndexDSN:   indexDSN,
			SourceDSN:  sourceDSN,
			ServerID:   serverID,
			BatchSize:  200,
			Schemas:    schema,
			Checkpoint: 1,
			GapTimeout: 30,
			Format:     "text",
			SSLMode:    "preferred",
			Deps: streamrun.Deps{
				ValidateBinlogFormat:   metadata.ValidateBinlogFormat,
				ValidateBinlogRowImage: metadata.ValidateBinlogRowImage,
				ValidateNoFKCascades:   metadata.ValidateNoFKCascades,
				ParseSchemaList:        cliutil.ParseSchemaList,
				ResolveServerIdentity:  byos.ResolveServerIdentity,
				EnsureResolver:         metadata.EnsureResolver,
				BuildIndexFilters:      cliutil.BuildIndexFilters,
				InsertSchemaChange:     indexer.InsertSchemaChange,
				ParseSourceDSN:         config.ParseSourceDSN,
				OutputJSON:             cliutil.OutputJSON,
				DetectSourceFlavor:     metadata.DetectSourceFlavor,
			},
		})
	}()
	stop = func() error {
		cancel()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("capture exited: %w", err)
			}
			return nil
		case <-time.After(30 * time.Second):
			return errors.New("the capture did not stop")
		}
	}
	if _, err := WaitCaptured(ctx, indexDB, errCh, Position{}, 60*time.Second); err != nil {
		stopErr := stop()
		return nil, nil, errors.Join(fmt.Errorf("the capture saved no first checkpoint: %w", err), stopErr)
	}
	return errCh, stop, nil
}
