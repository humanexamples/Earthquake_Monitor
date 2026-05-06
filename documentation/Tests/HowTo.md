**Bash-Commands for Testing:**



```bash
cd bronze-silver-gold-layer
docker compose --env-file .env --profile test run --rm test-runner
```

```bash
# Nur Bronze + Silver
docker compose --env-file .env --profile test run --rm test-runner pytest tests/test_bronze_silver.py -v

# Nur Gold-Infrastruktur
docker compose --env-file .env --profile test run --rm test-runner pytest tests/test_gold_infra.py -v

# Einzelner Test
docker compose --env-file .env --profile test run --rm test-runner pytest tests/test_gold_infra.py::TestSpark::test_worker_registered -v
```