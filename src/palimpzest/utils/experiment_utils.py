"""Small experiment utilities shared by repro scripts.

Currently contains a single helper to normalize human-readable plan text
blocks so different repro scripts can produce consistent `1.txt` files.
"""
from __future__ import annotations

import re
from typing import Iterable
import os
import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score


def load_ground_truth(workload: str, gt_path: str, dataset_dir: str) -> dict:
    if not gt_path or not os.path.exists(gt_path):
        return {}

    try:
        df = pd.read_csv(gt_path)
        df.columns = [c.strip().lower() for c in df.columns]
    except Exception:
        return {}

    if workload == "enron":
        actual_files = set(os.listdir(dataset_dir)) if os.path.exists(dataset_dir) else set()
        gt = {}
        for _, row in df.iterrows():
            fname = None
            for cand in ("filename", "file", "name"):
                if cand in df.columns:
                    fname = str(row.get(cand, "")).strip()
                    break
            if not fname:
                fname = str(row.iloc[0]).strip()

            label = None
            for cand in ("label", "y", "target"):
                if cand in df.columns:
                    try:
                        label = int(row.get(cand, 1))
                    except Exception:
                        label = 1
                    break
            if label is None:
                label = 1

            if fname in actual_files:
                gt[fname] = label

        return gt

    if workload == "real-estate":
        actual_listings = set(os.listdir(dataset_dir)) if os.path.exists(dataset_dir) else set()
        gt = {}
        label_col = "label" if "label" in df.columns else None
        for _, row in df.iterrows():
            listing_id = None
            for cand in ("listing", "filename", "id", "name"):
                if cand in df.columns:
                    listing_id = row.get(cand)
                    break
            if not listing_id:
                continue
            listing_id = str(listing_id).strip()
            label = int(row[label_col]) if label_col else 1
            if listing_id in actual_listings:
                gt[listing_id] = label
        return gt

    if workload == "medical-schema-matching":
        # Expect a CSV with target_attribute as first column and authors as additional columns
        gt_map = {}
        authors = [c for c in df.columns[1:]]
        for author in authors:
            key = author.lower().strip()
            gt_map[key] = {}
            for _, row in df.iterrows():
                attr = row[df.columns[0]] if len(df.columns) > 0 else None
                if attr is None:
                    continue
                original_col = str(row[author])
                should_exist = (original_col.lower() != "missing" and original_col.lower() != "nan")
                gt_map[key][str(attr).strip()] = should_exist
        return gt_map

    return {}


def compute_f1(results, gt_data, workload) -> float:
    if not gt_data:
        return 0.0

    y_true, y_pred = [], []

    if workload in ["enron", "real-estate"]:
        key_attr = "filename" if workload == "enron" else "listing"
        predicted_positives = {getattr(r, key_attr).strip() for r in results if hasattr(r, key_attr)}
        
        for item_id, label in gt_data.items():
            y_true.append(label)
            y_pred.append(1 if item_id in predicted_positives else 0)

    elif workload == "medical-schema-matching":
        for record in results:
            fname = getattr(record, 'filename', '') or ''
            matched_key = next((k for k in gt_data.keys() if k in fname.lower()), None)
            if not matched_key: continue
            
            targets = gt_data[matched_key]
            for attr, should_exist in targets.items():
                val = getattr(record, attr, None)
                has_value = 1 if (val is not None and str(val).strip() != "") else 0
                y_true.append(1 if should_exist else 0)
                y_pred.append(has_value)

    if not y_true: return 0.0

    avg_method = 'binary' if workload != "medical-schema-matching" else 'micro'
    p = precision_score(y_true, y_pred, average=avg_method, zero_division=0)
    r = recall_score(y_true, y_pred, average=avg_method, zero_division=0)
    f1 = f1_score(y_true, y_pred, average=avg_method, zero_division=0)
    
    return f1


def write_results_and_plans(
    results_iterable,
    logical_plan,
    gt_data: dict,
    output_csv: str,
    plan_details_dir: str,
    workload: str = "enron"
):
    all_results = []

    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
    os.makedirs(plan_details_dir, exist_ok=True)

    # allow generators by coercing to list
    try:
        results = list(results_iterable)
    except Exception:
        # if it's not iterable, attempt to wrap
        results = [results_iterable]

    for idx, res in enumerate(results, start=1):
        # gather timing/cost with several fallbacks
        cost = 0.0
        time_sec = 0.0
        try:
            es = getattr(res, "execution_stats", None)
            if es is not None and getattr(es, "plan_stats", None):
                first_ps = next(iter(es.plan_stats.values()))
                # try multiple attribute names used across versions
                for c_attr in ("total_plan_cost", "total_cost", "cost", "plan_cost"):
                    val = getattr(first_ps, c_attr, None)
                    if val is not None:
                        cost = val
                        break
                for t_attr in ("total_plan_time", "total_time", "time", "plan_time"):
                    val = getattr(first_ps, t_attr, None)
                    if val is not None:
                        time_sec = val
                        break
        except Exception:
            pass

        # metrics: try several ways to obtain a DataFrame
        df = pd.DataFrame(res)

        f1 = compute_f1(res, gt_data, workload)
            
        exec_stats = res.execution_stats

        stats = list(exec_stats.plan_stats.values())[0]

        plan_texts = stats.plan_str

        plan_file = os.path.join(plan_details_dir, f"{idx}.txt")
        try:
            with open(plan_file, "w", encoding="utf-8") as f:
                f.write(plan_texts)
        except Exception:
            pass

        all_results.append(
            {
                "Workload": workload,
                "Strategy": f"Plan-{idx}",
                "Time (s)": time_sec,
                "Cost ($)": cost,
                "F1 Score": f1,
                "Plan_File": f"{idx}.txt",
            }
        )

    # save CSV
    df_out = pd.DataFrame(all_results)
    df_out.to_csv(output_csv, index=False)
    print(f"Saved results to {output_csv}")
    print(f"Plan details saved in {plan_details_dir}")


__all__ = ["load_ground_truth", "compute_f1", "write_results_and_plans"]
