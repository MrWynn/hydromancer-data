from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    prefix: str
    target_table: str
    source_columns: Tuple[str, ...]


COMMON_COLUMNS = (
    "snapshot_date",
    "snapshot_block",
    "snapshot_timestamp",
    "updated_at",
)

DATASETS: Dict[str, DatasetSpec] = {
    "perp": DatasetSpec(
        name="perp",
        prefix="global/snapshots/perp/all",
        target_table="hydromancer_snapshot_perp",
        source_columns=(
            "user", "market", "size", "notional", "entry_price",
            "liquidation_price", "leverage_type", "leverage", "funding_pnl",
            "account_value", "account_mode",
        ),
    ),
    "spot": DatasetSpec(
        name="spot",
        prefix="global/snapshots/spot",
        target_table="hydromancer_snapshot_spot",
        source_columns=("user", "token", "balance", "entry_value"),
    ),
    "account_values": DatasetSpec(
        name="account_values",
        prefix="global/snapshots/account_values",
        target_table="hydromancer_snapshot_account_values",
        source_columns=(
            "user", "dex", "collateral_token", "account_value",
            "total_long_notional", "total_short_notional", "account_mode",
            "is_unified", "pm_ratio", "pm_avail_by_branch_json",
            "pm_total_account_value",
        ),
    ),
}

TASK_TABLE = "hydromancer_sync_tasks"
INGESTION_TASK_TABLE = "hyperliquid_s3_ingestion_tasks"
NODE_FILLS_TABLE = "hyperliquid_s3_node_fills_by_block_fills"
ALLOWED_TABLES = frozenset(
    [TASK_TABLE, INGESTION_TASK_TABLE, NODE_FILLS_TABLE]
    + [spec.target_table for spec in DATASETS.values()]
)

