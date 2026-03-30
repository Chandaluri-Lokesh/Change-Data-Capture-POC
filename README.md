# poc-debezium (Windows + WSL only)

This project is a native (non-Docker) Debezium CDC POC intended to run on:

- **Windows**: MongoDB + Python apps
- **WSL**: Kafka (**KRaft mode**, no ZooKeeper), Kafka Connect + Debezium MongoDB plugin

## Runbook

Use `LOCAL_EXECUTION_GUIDE.md` as the source of truth.

## Notes

- Docker artifacts have been retired into `deprecated/` for reference.
- Debezium connector configs in `connectors/` target `localhost` MongoDB (Windows) from WSL.
