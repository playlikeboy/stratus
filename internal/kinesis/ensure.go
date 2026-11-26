package kinesis

// ShardCount is the number of shards of a stream, and whether it exists.
func (s *Service) ShardCount(name string) (int, bool) {
	s.mu.RLock()
	st, ok := s.streams[name]
	s.mu.RUnlock()
	if !ok {
		return 0, false
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.shards), true
}
