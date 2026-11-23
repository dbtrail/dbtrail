//go:build integration

package console

import "sync"

// HoldTailsUpdateForTest makes server id's tail keeper look as it does while
// an update of its tails runs, until release is called: its lock is held and
// the update is marked as running, so statements read what is published and
// do not start one of their own. It says the directory of the tails. ok is
// false when the server has no keeper or no directory yet. Test helper ONLY,
// built only for the integration tests of this and other packages.
func (s *Server) HoldTailsUpdateForTest(id string) (release func(), dir string, ok bool) {
	s.tails.mu.Lock()
	k := s.tails.by[id]
	s.tails.mu.Unlock()
	if k == nil {
		return nil, "", false
	}
	k.mu.Lock()
	if k.dir == "" {
		k.mu.Unlock()
		return nil, "", false
	}
	p := &k.pub
	p.mu.Lock()
	marked := !p.updating
	if marked {
		p.gen++
		p.updating, p.stuck, p.started = true, false, k.now()
	}
	gen := p.gen
	p.mu.Unlock()
	return sync.OnceFunc(func() {
		if marked {
			p.mu.Lock()
			// Unless a real update started meanwhile, which says its own end.
			if p.gen == gen {
				p.updating = false
			}
			p.markPublishedLocked()
			p.mu.Unlock()
		}
		k.mu.Unlock()
	}), k.dir, true
}
