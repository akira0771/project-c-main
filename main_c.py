main.py
﻿import logging
import sys
import pandas as pd

from config import (
    YOUTUBE_MASTER_FILE_ID,
    ATTENDANCE_MASTER_FILE_ID,
    NO_TO_OUTPUT_FILE_ID,
    LOG_FILE_NAME,
)
from drive_client import build_drive_service, download_csv_to_dataframe, update_file_with_dataframe
from processing import validate_and_normalize_youtube, build_final_output, dedup_and_sort


def setup_logging():
    logging.basicConfig(
        filename=LOG_FILE_NAME,
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.ERROR)
    console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(console)


def log_error(message, e):
    msg = f"{message}: {e}"
    logging.error(msg)
    print(f"ERROR: {msg}")


def main_processor():
    setup_logging()
    try:
        service = build_drive_service()
    except Exception as e:
        log_error("Google Drive 認証に失敗", e)
        sys.exit(1)

    # 入力CSV取得
    df_y_raw = download_csv_to_dataframe(service, YOUTUBE_MASTER_FILE_ID)
    df_a_raw = download_csv_to_dataframe(service, ATTENDANCE_MASTER_FILE_ID)
    if df_y_raw is None or df_a_raw is None:
        print("致命的エラー: 入力データのダウンロードに失敗しました")
        sys.exit(1)

    # 整形・結合（正規化→日内最新→結合→列固定）
    try:
        df_y = validate_and_normalize_youtube(df_y_raw)
        final_df = build_final_output(df_y, df_a_raw)
        final_df = dedup_and_sort(final_df)
        print(f"整形と結合完了: {len(final_df)} 行")
    except Exception as e:
        log_error("結合/加工でエラー", e)
        sys.exit(1)

    # 既存CSV更新（no ごと）
    for no_value in sorted(final_df["no"].dropna().unique()):
        file_id = NO_TO_OUTPUT_FILE_ID.get(int(no_value))
        if not file_id:
            log_error(f"出力ファイルID未設定: no={no_value}", Exception("missing output file id"))
            continue

        df_new = final_df[final_df["no"] == no_value].copy()
        try:
            df_exist = download_csv_to_dataframe(service, file_id)
            if df_exist is not None and not df_exist.empty:
                df_out = pd.concat([df_exist, df_new], ignore_index=True)
            else:
                df_out = df_new
            # 出力列固定
            df_out = df_out[[c for c in ["date", "no", "title", "views", "seats"] if c in df_out.columns]]
            if set(["title", "date"]).issubset(df_out.columns):
                df_out.drop_duplicates(subset=["title", "date"], keep="last", inplace=True)
            sort_cols = [c for c in ["no", "date"] if c in df_out.columns]
            if sort_cols:
                df_out = df_out.sort_values(by=sort_cols)
            update_file_with_dataframe(service, file_id, df_out)
            print(f"更新: no={no_value}, 追加後 {len(df_out)} 行")
        except Exception as e:
            log_error(f"更新時エラー: no={no_value}", e)
            continue

    print("完了: 既存ファイルを更新")


if __name__ == "__main__":
    main_processor()
