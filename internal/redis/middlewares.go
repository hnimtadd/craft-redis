package redis

import (
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/redis/state/replication"
)

func (c *Controller) ReplicaMiddleware(next HandlerFunc) HandlerFunc {
	return func(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
		res, err := next(cmd, session)
		if err != nil {
			return res, err
		}
		go func() {
			c.logger.Debug("propagating to replicas...")
			datas := make([]resp.Data, len(cmd.args)+1)
			datas[0] = cmd.cmd
			for idx, arg := range cmd.args {
				datas[idx+1] = arg
			}
			fullCmd := resp.ArraysData{Datas: datas}
			handler := func(sessionHash string, r *replication.Replica) (shouldStop bool) {
				c.logger.Debug("propagating to " + sessionHash)
				if !r.IsReady {
					c.logger.Debug("replicate is not ready, skipping")
					return true
				}
				c.logger.Debug("replica is ready, sending...")
				if r.Conn == nil {
					c.logger.Debug("connection is nil, skipping...")
					return true
				}
				// Use Write instead of WriteThenRead - replicas don't respond to propagated commands
				err := r.Conn.Write([]byte(fullCmd.String()))
				if err != nil {
					c.logger.Debug("failed to write to tcp connection", "err", err)
				}
				c.logger.Debug("sent")
				// returning true means we continue
				return true
			}
			c.replicationState.replicas.ForEach(handler)
		}()
		return res, nil
	}
}

func (c *Controller) MULTIMiddleware(next HandlerFunc) HandlerFunc {
	return func(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
		if !c.queue.Has(session.Hash) {
			return next(cmd, session)
		}
		queue, _ := c.queue.Get(session.Hash)
		queue.commands = append(queue.commands, QueueCommand{
			handler: next,
			cmd:     cmd,
			session: session,
		})
		return resp.SimpleStringData{Data: "QUEUED"}, nil
	}
}

// ReplicationConnectionMiddleware detect if the command should be return or could be silently ignore.
func (c *Controller) ReplicationConnectionMiddleware(next HandlerFunc) HandlerFunc {
	return func(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
		res, err := next(cmd, session)
		if session.IsReplicationSession {
			if err != nil {
				return nil, err
			}
			if (strings.ToUpper(cmd.cmd.Data) == "REPLCONFG") &&
				len(cmd.args) == 2 &&
				strings.ToUpper(cmd.args[0].Data) == "GETACK" {
				return res, nil
			}
			return nil, nil
		}

		if err != nil {
			return nil, err
		}
		return res, nil
	}
}
