package dynamodb

type ttlSpecification struct {
	AttributeName string
	Enabled       bool
}

type updateTimeToLiveInput struct {
	TableName               string
	TimeToLiveSpecification *ttlSpecification
}

// updateTimeToLiveOp enables or disables TTL. The change is immediate; items
// are deleted only by ExpireTTL.
func (s *Service) updateTimeToLiveOp(in updateTimeToLiveInput) (any, error) {
	spec := in.TimeToLiveSpecification
	if spec == nil || spec.AttributeName == "" {
		return nil, errValidation("1 validation error detected: Value null at 'timeToLiveSpecification' " +
			"failed to satisfy constraint: Member must not be null")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(in.TableName)
	if err != nil {
		return nil, err
	}
	switch {
	case spec.Enabled && t.ttl == spec.AttributeName:
		return nil, errValidation("TimeToLive is already enabled")
	case spec.Enabled && t.ttl != "":
		return nil, errValidation("TimeToLive is active on a different AttributeName: current AttributeName is %s",
			t.ttl)
	case !spec.Enabled && t.ttl == "":
		return nil, errValidation("TimeToLive is already disabled")
	case !spec.Enabled && t.ttl != spec.AttributeName:
		return nil, errValidation("TimeToLive is active on a different AttributeName: current AttributeName is %s",
			t.ttl)
	}
	if spec.Enabled {
		t.ttl = spec.AttributeName
	} else {
		t.ttl = ""
	}
	return map[string]ttlSpecification{"TimeToLiveSpecification": *spec}, nil
}

type ttlDescription struct {
	TimeToLiveStatus string
	AttributeName    string `json:",omitempty"`
}

func (s *Service) describeTimeToLiveOp(in tableNameInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(in.TableName)
	if err != nil {
		return nil, err
	}
	d := ttlDescription{TimeToLiveStatus: "DISABLED"}
	if t.ttl != "" {
		d = ttlDescription{TimeToLiveStatus: "ENABLED", AttributeName: t.ttl}
	}
	return map[string]ttlDescription{"TimeToLiveDescription": d}, nil
}
