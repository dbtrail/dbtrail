package reconstruct

// reset forgets every floor: a test that needs a first search.
func (s *cutFloorStore) reset() {
	s.mu.Lock()
	s.m = map[string]cutFloor{}
	s.mu.Unlock()
}
