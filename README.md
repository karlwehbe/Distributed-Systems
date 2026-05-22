# Distributed Systems

Coursework for **COMP 512** (Distributed Systems). Each top-level directory corresponds to a core distributed systems technology.

## Projects

### [TCP_middleware](TCP_middleware/)

Client–middleware–server over **RMI** and **TCP**, with distributed resource managers for flights, cars, and rooms.

- Code: `TCP_middleware/p1-students/Template/`
- Report: `TCP_middleware/P1_Report.pdf`
- See [TCP_middleware/p1-students/Template/README.md](TCP_middleware/p1-students/Template/README.md) for build and run instructions.

### [paxos_algo_system](paxos_algo_system/)

**Paxos** consensus for replicated state, applied to a multi-player application over the group communication layer (GCL).

- Code: `paxos_algo_system/p2-students/comp512p2/comp512st/`
- Docs: `paxos_algo_system/p2-students/p2docs/`
- Main packages: `paxos/`, `tiapp/`, `tests/` (under `comp512st/`)

### [apache_zookeeper](apache_zookeeper/)

**Apache ZooKeeper** coordination: a manager assigns tasks to workers via znodes.

## Repository layout

```
.
├── TCP_middleware/
├── paxos_algo_system/
└── apache_zookeeper/
```

## Requirements

- **Java** (versions depend on each assignment’s provided libraries and templates)
- **apache_zookeeper**: Apache ZooKeeper client libraries and a reachable ZK ensemble
