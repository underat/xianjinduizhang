# -*- coding: utf-8 -*-
"""
Created on Wed Jun  4 10:15:00 2025

@author: leonx
"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
现金日记账  vs  金蝶日记账   自动核对脚本
作者：ChatGPT（2025-06-04）
------------------------------------------------------------------
逻辑概要
1. 读取两本 Excel
2. 现金账：借方为正、贷方取负；合并“类型+业务日期+报表人”→ 摘要
   · “市民住院预交款”按业务日期汇总
3. 金蝶账：借方为正、贷方取负；保留原“摘要”
4. 模糊比对  
   · 摘要相似度 > 0.2（20 %）  
   · 金额差 |Δ| < 0.01  
5. 输出匹配、未匹配列表（Excel）
------------------------------------------------------------------
"""

from pathlib import Path
import pandas as pd
from rapidfuzz import fuzz

# ★ 路径自行修改 / 使用命令行参数皆可
CASH_XLSX = Path(r"C:\Users\leonx\Documents\BaiduSyncdisk\华东医院\出纳工作流程\现金日记账核对\现金日记账.xlsx")
K3_XLSX   = Path(r"C:\Users\leonx\Documents\BaiduSyncdisk\华东医院\出纳工作流程\现金日记账核对\金蝶日记账.xlsx")
OUTPUT    = CASH_XLSX.parent / "核对结果.xlsx"

SIM_THRESHOLD  = 0.2      # 摘要相似度
AMT_TOLERANCE  = 0.01     # 金额绝对误差


def _load_excel_autoheader(path: Path, required: list[str]) -> pd.DataFrame:
    """Load Excel and automatically locate the header row.

    Some exported spreadsheets contain a few introductory rows before the
    actual data.  The previous implementation simply defaulted to row ``0``
    when the required columns were not found which resulted in ``Unnamed``
    column names.  To be more robust we now scan all rows and look for cells
    that *contain* the required text.  If no such row exists, an explicit
    ``ValueError`` is raised so the caller can provide a clearer error.
    """

    preview = pd.read_excel(path, header=None, dtype=str).fillna("")
    header_row: int | None = None

    for idx, row in preview.iterrows():
        cols = [str(c).strip() for c in row.tolist()]
        if all(any(r in c for c in cols) for r in required):
            header_row = idx
            break

    if header_row is None:
        raise ValueError(
            f"在文件 {path} 中未找到包含 {required} 的表头"
        )

    df = pd.read_excel(path, header=header_row, dtype=str).fillna("")
    df.rename(columns=lambda x: str(x).strip(), inplace=True)
    return df


# ---------- 1. 读取 ----------
def read_cash(path: Path) -> pd.DataFrame:
    df = _load_excel_autoheader(path, ["借方", "贷方"])
    # 转数值列
    for col in ["借方", "贷方"]:
        try:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        except KeyError:
            raise KeyError(f"缺少列 '{col}'，实际列: {list(df.columns)}") from None
    return df


def read_k3(path: Path) -> pd.DataFrame:
    df = _load_excel_autoheader(path, ["借方", "贷方"])
    for col in ["借方", "贷方"]:
        try:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        except KeyError:
            raise KeyError(f"缺少列 '{col}'，实际列: {list(df.columns)}") from None
    return df

# ---------- 2. 现金账处理 ----------
def preprocess_cash(df: pd.DataFrame) -> pd.DataFrame:
    # 特殊汇总：市民住院预交款
    mask_prepay = df["类型"].str.contains("市民住院预交款", na=False)
    if mask_prepay.any():
        grp = (
            df[mask_prepay]
            .groupby("业务日期", as_index=False)[["借方", "贷方"]]
            .sum()
            .assign(类型="市民住院预交款", 报表人="汇总")
        )
        df = pd.concat([df.loc[~mask_prepay], grp], ignore_index=True)

    # 生成摘要
    df["摘要"] = df["类型"].str.strip() + "-" + df["业务日期"].astype(str).str.strip() + "-" + df["报表人"].str.strip()

    # 统一金额：借方正、贷方负
    df["金额"] = df["借方"] - df["贷方"]
    return df[["业务日期", "摘要", "金额"]]


# ---------- 3. 金蝶账处理 ----------
def preprocess_k3(df: pd.DataFrame) -> pd.DataFrame:
    df["金额"] = df["借方"] - df["贷方"]
    return df[["业务日期", "摘要", "金额"]]


# ---------- 4. 核对 ----------
def reconcile(cash: pd.DataFrame, k3: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    matched_rows = []
    used_cash_idx = set()
    used_k3_idx  = set()

    for i, c_row in cash.iterrows():
        best_match = None
        best_score = 0.0

        for j, k_row in k3.iterrows():
            if j in used_k3_idx:
                continue
            if abs(c_row["金额"] - k_row["金额"]) > AMT_TOLERANCE:
                continue
            score = fuzz.token_sort_ratio(c_row["摘要"], k_row["摘要"]) / 100.0
            if score >= SIM_THRESHOLD and score > best_score:
                best_score, best_match = score, j

        if best_match is not None:
            used_k3_idx.add(best_match)
            used_cash_idx.add(i)
            k_row = k3.loc[best_match]
            matched_rows.append(
                {
                    "业务日期":   c_row["业务日期"],
                    "现金摘要":   c_row["摘要"],
                    "金蝶摘要":   k_row["摘要"],
                    "现金金额":   c_row["金额"],
                    "金蝶金额":   k_row["金额"],
                    "摘要相似度": round(best_score, 4),
                    "金额差":     round(c_row["金额"] - k_row["金额"], 4),
                }
            )

    matched_df   = pd.DataFrame(matched_rows)
    unmatched_df = cash.drop(index=list(used_cash_idx), errors="ignore")
    unmatched_k3 = k3.drop(index=list(used_k3_idx), errors="ignore")

    return matched_df, unmatched_df, unmatched_k3


# ---------- 5. 主流程 ----------
def main():
    cash_raw = read_cash(CASH_XLSX)
    k3_raw   = read_k3(K3_XLSX)

    cash_df  = preprocess_cash(cash_raw)
    k3_df    = preprocess_k3(k3_raw)

    matched, cash_unm, k3_unm = reconcile(cash_df, k3_df)

    with pd.ExcelWriter(OUTPUT, engine="openpyxl") as xlw:
        matched.to_excel(xlw,   sheet_name="匹配成功", index=False)
        cash_unm.to_excel(xlw,  sheet_name="现金未匹配", index=False)
        k3_unm.to_excel(xlw,    sheet_name="金蝶未匹配", index=False)

    print(f"✅ 完成！结果保存至：{OUTPUT}")

# ------------- run -------------
if __name__ == "__main__":
    main()
