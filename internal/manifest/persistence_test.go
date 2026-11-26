package manifest

import "testing"

func TestPersistenceIsResolvedFromTheResourceUp(t *testing.T) {
	m := mustParse(t, `
persistence: false
sqs:
  persistence: true
  queues:
    - name: jobs
    - name: scratch
      persistence: false
dynamodb:
  tables:
    - name: orders
      partitionKey: {name: id}
      persistence: true
    - name: cache
      partitionKey: {name: id}
`)
	p := m.PersistenceFor(true)
	for _, tc := range []struct {
		service, name string
		want          bool
	}{
		{ServiceSQS, "jobs", true},           // the service's
		{ServiceSQS, "scratch", false},       // its own
		{ServiceSQS, "made-by-a-test", true}, // not declared: the service's
		{ServiceDynamoDB, "orders", true},    // its own
		{ServiceDynamoDB, "cache", false},    // the manifest's
		{ServiceKinesis, "events", false},    // the manifest's
	} {
		if got := p.Keeps(tc.service, tc.name); got != tc.want {
			t.Errorf("Keeps(%s, %s) = %v, want %v", tc.service, tc.name, got, tc.want)
		}
	}
	if !p.Any() {
		t.Error("Any() = false with persistent resources")
	}
}

func TestPersistenceDefaultsToWhetherThereIsAStateDirectory(t *testing.T) {
	m := mustParse(t, "sqs:\n  queues:\n    - name: jobs\n")
	if !m.PersistenceFor(true).Keeps(ServiceSQS, "jobs") {
		t.Error("with a state directory and nothing said, a queue is not kept")
	}
	m = mustParse(t, "persistence: true\nsqs:\n  queues:\n    - name: jobs\n      persistence: true\n")
	if p := m.PersistenceFor(false); p.Keeps(ServiceSQS, "jobs") || p.Any() {
		t.Error("without a state directory, a resource is kept")
	}
}
