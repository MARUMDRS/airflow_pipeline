import json
from pathlib import Path
from notebook_replayer.config import TRAINING_OUTPUT_DIR
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def export_step(
    output_dir: Path,
    step_id: str,
    input_df,
    output_df,
    op_name,
    args_dict,
    raw_code=None,
):
    """
    Save the transformation inputs, outputs, and parameters.
    """
    step_path = output_dir / f"step_{step_id}"
    step_path.mkdir(parents=True, exist_ok=True)

    input_path = step_path / "input.csv"
    output_path = step_path / "data.csv"
    param_path = step_path / "param.json"
    meta_path = step_path / "metadata.json"

    if input_df is not None:
        input_df.to_csv(input_path, index=False)
    if output_df is not None:
        output_df.to_csv(output_path, index=False)

    with open(param_path, "w") as f:
        json.dump({"op": op_name, "args": args_dict}, f, indent=2)

    with open(meta_path, "w") as f:
        json.dump(
            {
                "op": op_name,
                "args": args_dict,
                "raw_code": raw_code or "",
            },
            f,
            indent=2,
        )


def export_training_example(df: "pd.DataFrame", op_signature: dict, export_dir: Path):
    try:
        if df is None or df.empty:
            logger.warning(f"[EXPORT] Skipping export — empty DataFrame")
            return

        export_dir.mkdir(parents=True, exist_ok=True)
        logger.warning(f"[EXPORT] Writing to: {export_dir}")

        df.to_csv(export_dir / "data.csv", index=False)
        with open(export_dir / "param.json", "w") as f:
            json.dump(op_signature, f, indent=2)
        with open(export_dir / "metadata.json", "w") as f:
            json.dump(
                {
                    "op": op_signature.get("op"),
                    "args": op_signature.get("args"),
                    "df": op_signature.get("df"),
                },
                f,
                indent=2,
            )

    except Exception as e:
        logger.error(f"[EXPORT] Failed to write export to {export_dir}: {e}")


def cleanup_notebook_export_dir(notebook_name: str):
    """
    Remove notebook export folder ONLY if it exists and is already empty.
    Does not create any path. Silent if nothing was created.
    """
    export_path = Path(TRAINING_OUTPUT_DIR) / notebook_name

    if not export_path.exists():
        # ✅ Do nothing if the dir never existed
        return

    if export_path.is_dir() and not any(export_path.iterdir()):
        try:
            export_path.rmdir()
            logger.warning(f"[CLEANUP] Removed empty export dir: {export_path}")
        except Exception as e:
            logger.warning(f"[CLEANUP] Failed to remove {export_path}: {e}")


def export_autosuggest_example(
    input_df: pd.DataFrame,
    op_name: str,
    repo_id: str,
    notebook_name: str,
    cell_id: int,
    op_id: int,
    args_dict: dict,
    output_dir_base: Path = Path("training_data"),
):
    """
    Exports an operator invocation to the AutoSuggest dataset format:
    training_data/[OPERATOR]/[REPO]__[NOTEBOOK]_cell[CELLID]_[OPID]/
        ├── data.csv       ← the input DataFrame
        └── param.json     ← JSON with the arguments to the operator
    """
    # Clean names
    repo_clean = repo_id.replace("/", "__")
    notebook_clean = notebook_name.replace(".ipynb", "")

    # Directory path as specified in the paper
    export_dir = (
        output_dir_base
        / op_name
        / f"{repo_clean}_{notebook_clean}_cell{cell_id}_{op_id}"
    )
    export_dir.mkdir(parents=True, exist_ok=True)

    # Export CSV
    input_df.to_csv(export_dir / "data.csv", index=False)

    # Export operator parameters
    with open(export_dir / "param.json", "w", encoding="utf-8") as f:
        json.dump(args_dict, f, indent=2)

def cleanup_all_empty_dirs(base_path: Path):
    """
    Recursively removes all empty folders under the given base_path.
    """
    if not base_path.exists():
        return

    for subdir in base_path.rglob("*"):
        if subdir.is_dir() and not any(subdir.iterdir()):
            try:
                subdir.rmdir()
                logger.info(f"[CLEANUP] Removed empty directory: {subdir}")
            except Exception as e:
                logger.warning(f"[CLEANUP] Failed to remove {subdir}: {e}")
