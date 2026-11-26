package manifest

// The services, as Persistence names them.
const (
	ServiceSQS           = "sqs"
	ServiceKinesis       = "kinesis"
	ServiceS3            = "s3"
	ServiceDynamoDB      = "dynamodb"
	ServiceStepFunctions = "stepFunctions"
)

// Persistence says which resources keep their data across a restart.
type Persistence struct {
	enabled   bool // a state directory is given: without one nothing is kept
	global    *bool
	services  map[string]*bool
	resources map[string]map[string]bool
}

// PersistenceFor resolves the manifest's persistence settings for an emulator
// with a state directory (stateDir) or without one. A resource is kept by its
// own setting, else its service's, else the manifest's, else whenever there
// is a state directory. A resource the manifest does not declare, created
// through the API, follows its service.
func (m Manifest) PersistenceFor(stateDir bool) Persistence {
	p := Persistence{
		enabled: stateDir, global: m.Persistence,
		services: map[string]*bool{
			ServiceSQS: m.SQS.Persistence, ServiceKinesis: m.Kinesis.Persistence, ServiceS3: m.S3.Persistence,
			ServiceDynamoDB: m.DynamoDB.Persistence, ServiceStepFunctions: m.StepFunctions.Persistence,
		},
		resources: map[string]map[string]bool{},
	}
	set := func(service, name string, v *bool) {
		if v == nil {
			return
		}
		if p.resources[service] == nil {
			p.resources[service] = map[string]bool{}
		}
		p.resources[service][name] = *v
	}
	for _, q := range m.SQS.Queues {
		set(ServiceSQS, q.Name, q.Persistence)
	}
	for _, s := range m.Kinesis.Streams {
		set(ServiceKinesis, s.Name, s.Persistence)
	}
	for _, b := range m.S3.Buckets {
		set(ServiceS3, b.Name, b.Persistence)
	}
	for _, t := range m.DynamoDB.Tables {
		set(ServiceDynamoDB, t.Name, t.Persistence)
	}
	for _, sm := range m.StepFunctions.StateMachines {
		set(ServiceStepFunctions, sm.Name, sm.Persistence)
	}
	return p
}

// Keeps reports whether the resource name of service keeps its data.
func (p Persistence) Keeps(service, name string) bool {
	if !p.enabled {
		return false
	}
	if v, ok := p.resources[service][name]; ok {
		return v
	}
	return p.service(service)
}

func (p Persistence) service(service string) bool {
	if v := p.services[service]; v != nil {
		return *v
	}
	if p.global != nil {
		return *p.global
	}
	return true
}

// Any reports whether any resource, declared or not, may keep its data.
func (p Persistence) Any() bool {
	if !p.enabled {
		return false
	}
	for service := range p.services {
		if p.service(service) {
			return true
		}
	}
	for _, names := range p.resources {
		for _, keep := range names {
			if keep {
				return true
			}
		}
	}
	return false
}

// Filter is Keeps for one service, in the form a service's Snapshot takes.
func (p Persistence) Filter(service string) func(name string) bool {
	return func(name string) bool { return p.Keeps(service, name) }
}
