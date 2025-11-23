"""
Metadata module
---------------

Abstração local (JSON) para metadados de ingestão e checkpoints,
pensada para ser facilmente migrada para DynamoDB.

Funções principais expostas:
- start_run(run_scope)
- end_run(ingestion_run_id, ...)
- save_checkpoint(source, value)
- load_checkpoint(source, default=None)

Exemplos de uso (local):

    from metadata import (
        start_run,
        end_run,
        save_checkpoint,
        load_checkpoint,
        WORLD_BANK_API_SCOPE,
    )

    run_id = start_run(WORLD_BANK_API_SCOPE)
    # ... executar ingestão ...
    end_run(run_id, status=\"SUCCESS\", rows_processed=1234, last_checkpoint=\"2023\")

    last_year = load_checkpoint(WORLD_BANK_API_SCOPE, default=\"2000\")
"""

from .store import (
    DEFAULT_METADATA_FILE,
    METADATA_LOCAL_FILE_ENV,
    end_run,
    get_all_checkpoints,
    get_last_run,
    list_runs,
    load_checkpoint,
    reset_local_store,
    save_checkpoint,
    start_run,
)

# Run scopes sugeridos pelo plano
WORLD_BANK_API_SCOPE = "world_bank_api"
WIKIPEDIA_CO2_SCOPE = "wikipedia_co2"
CURATED_JOIN_SCOPE = "curated_join"

__all__ = [
    "DEFAULT_METADATA_FILE",
    "METADATA_LOCAL_FILE_ENV",
    "WORLD_BANK_API_SCOPE",
    "WIKIPEDIA_CO2_SCOPE",
    "CURATED_JOIN_SCOPE",
    "start_run",
    "end_run",
    "save_checkpoint",
    "load_checkpoint",
    "get_last_run",
    "list_runs",
    "get_all_checkpoints",
    "reset_local_store",
]

