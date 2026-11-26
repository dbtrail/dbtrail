package consoleapp

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// pgJoinStats is a port's pgshim.JoinStats, one per server and forwarding
// account: a server's column statistics are its own source's, as that
// account may read them, read on a connection of their own
// (pgReadJoinStats), never a client's, in the background.
type pgJoinStats struct{ by sync.Map }

func (p *pgJoinStats) of(id, dsn string, logger *slog.Logger) *pgshim.JoinStats {
	key := id + "\x00" + dsn
	if v, ok := p.by.Load(key); ok {
		return v.(*pgshim.JoinStats)
	}
	js := &pgshim.JoinStats{Server: id, Logger: logger, Read: func(ctx context.Context) (pgshim.JoinTables, error) {
		return pgReadJoinStats(ctx, dsn)
	}}
	v, _ := p.by.LoadOrStore(key, js)
	return v.(*pgshim.JoinStats)
}

// pgReadJoinStats reads a source's column statistics (pgshim.JoinStatsSQL)
// on a connection of its own, as the account dsn names.
func pgReadJoinStats(ctx context.Context, dsn string) (pgshim.JoinTables, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	var cell string
	if err := conn.QueryRow(ctx, pgshim.JoinStatsSQL).Scan(&cell); err != nil {
		return nil, err
	}
	return pgshim.ParseJoinStats([]byte(cell))
}
