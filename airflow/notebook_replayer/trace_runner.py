
import sys
import logging
import inspect
import pandas as pd
from pathlib import Path
from notebook_replayer.utils import export_step
from notebook_replayer.config import TRAINING_OUTPUT_DIR
from notebook_replayer.utils import export_autosuggest_example

from itertools import count

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

PANDAS_TRANSFORMS = {
    "merge", "groupby", "pivot", "melt", "join", "concat", "stack", "unstack",
    "pivot_table", "explode", "drop_duplicates", "value_counts", "crosstab"
}

step_counter = count()

def make_serializable_args(args_dict):
    """
    Convert args to a JSON-serializable format.
    DataFrames: replaced with shape info.
    Series: replaced with dtype and length.
    Functions/objects: replaced with string repr.
    """
    def serialize_value(val):
        if isinstance(val, pd.DataFrame):
            return {"type": "DataFrame", "shape": val.shape}
        elif isinstance(val, pd.Series):
            return {"type": "Series", "dtype": str(val.dtype), "length": len(val)}
        elif isinstance(val, (list, tuple)):
            return [serialize_value(v) for v in val]
        elif isinstance(val, dict):
            return {str(k): serialize_value(v) for k, v in val.items()}
        elif isinstance(val, (int, float, str, bool)) or val is None:
            return val
        else:
            return str(type(val).__name__)  # fallback: type name string

    return {k: serialize_value(v) for k, v in args_dict.items()}

def trace_pandas_ops(exec_env: dict, base_output_path: Path):
    """
    Returns a sys.settrace-compatible function that logs pandas transformations.
    """

    def trace_calls(frame, event, arg):
        if event != "call":
            return

        func_name = frame.f_code.co_name
        module_name = frame.f_globals.get("__name__", "")

        if not module_name.startswith("pandas"):
            return
        if func_name not in PANDAS_TRANSFORMS:
            return

        args_info = inspect.getargvalues(frame)
        args_dict = {arg: frame.f_locals[arg] for arg in args_info.args if arg in frame.f_locals}

        # Attempt to fetch raw code context
        raw_code = ""
        try:
            lineno = frame.f_lineno
            filename = frame.f_code.co_filename
            with open(filename, "r", encoding="utf-8") as f:
                lines = f.readlines()
                raw_code = lines[lineno - 1].strip()
        except Exception:
            pass

        op_index = [None]  # shared across calls

        def trace_returns(inner_frame, event, return_value):
            if event != "return" or not isinstance(return_value, pd.DataFrame):
                return

            try:
                caller_locals = frame.f_back.f_locals
                input_df = next((v for v in caller_locals.values() if isinstance(v, pd.DataFrame)), None)
                output_df = return_value

                if output_df is None or output_df.empty:
                    logger.info(f"[TRACE] Skipping empty output from {func_name}")
                    return

                if op_index[0] is None:
                    op_index[0] = next(step_counter)

                step_id = str(op_index[0])
                export_path = base_output_path / step_id
                export_path.mkdir(parents=True, exist_ok=True)

                safe_args = make_serializable_args(args_dict)

                export_step(
                    output_dir=base_output_path,
                    step_id=step_id,
                    input_df=input_df,
                    output_df=output_df,
                    op_name=func_name,
                    args_dict=safe_args,
                    raw_code=raw_code
                )

                parts = base_output_path.parts
                repo_id = parts[-3] if len(parts) >= 3 else "unknown_repo"
                notebook_name = parts[-2] if len(parts) >= 2 else "unknown_notebook"

                export_autosuggest_example(
                    input_df=input_df or output_df,
                    op_name=func_name,
                    repo_id=repo_id,
                    notebook_name=notebook_name,
                    cell_id=frame.f_lineno,
                    op_id=op_index[0],
                    args_dict=safe_args,
                    output_dir_base=Path("training_data")
                )

                logger.info(f"[TRACE] ✅ Logged pandas op '{func_name}' to {export_path}")

            except Exception as e:
                logger.error(f"[TRACE] ❌ Failed to trace pandas op '{func_name}': {e}")

        return trace_returns 

    return trace_calls