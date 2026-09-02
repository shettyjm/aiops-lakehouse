// Sample Cypher for PuppyGraph (M5, Option B) over the topology graph built from
// the AIStor Iceberg `topology` table. The default demo uses the copilot's
// trace_dependencies tool instead; this mirrors it in Cypher for the
// customer-story parity beat.

// 1. Blast radius: everything downstream of postgres-db (impacted if it fails).
MATCH path = (root:App {app: 'postgres-db'})<-[:DEPENDS_ON*1..5]-(impacted:App)
RETURN DISTINCT impacted.app AS impacted_app, length(path) AS hops
ORDER BY hops, impacted_app;

// 2. The dependency chain the other way: what patient-onboarding relies on.
MATCH path = (svc:App {app: 'patient-onboarding'})-[:DEPENDS_ON*1..5]->(dep:App)
RETURN DISTINCT dep.app AS depends_on, length(path) AS hops
ORDER BY hops;

// 3. Root-cause to symptom: does a failing dependency reach patient-onboarding?
MATCH p = (fault:App {app: 'postgres-db'})<-[:DEPENDS_ON*]-(sym:App {app: 'patient-onboarding'})
RETURN p;

// 4. Which VMs would be affected in the blast radius (App -> VM).
MATCH (root:App {app: 'postgres-db'})<-[:DEPENDS_ON*1..5]-(impacted:App)-[:RUNS_ON]->(vm:VM)
RETURN impacted.app AS app, collect(vm.vm_id) AS vms;
