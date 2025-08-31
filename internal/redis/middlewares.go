package redis

import (
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
			handler := func(sessionHash string, r *replication.Replica) bool {
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
				c.Send(r.Conn, fullCmd)
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
