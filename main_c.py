import pandas as pd
import io
import numpy as np
import sys 
import logging # ロギングモジュールをインポート
from datetime import datetime # 日時取得モジュールをインポート
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# --- Google Drive ファイルID 設定 ---
YOUTUBE_MASTER_FILE_ID = '1fZntJuEUcpTeXcbR5aGWvVj8WfsAE_Cb'
ATTENDANCE_MASTER_FILE_ID = '1EJeJ5215gngU_7YxlFnj_3kFA4q--Koe'
FINAL_ANALYSIS_FOLDER_ID = '1w8pmCqX2TxJHfearx1XBmm4XRAXsV8We'

# ロギング設定
LOG_FILE_NAME = 'process_error_log.txt'
YOUTUBE_RANKING_COLUMN = 'no' 

# --- ロギング関数 ---
def setup_logging():
    """ロギングを設定し、ファイルに出力するようにする"""
    logging.basicConfig(
        filename=LOG_FILE_NAME,
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # コンソールにもエラーを出力するためのハンドラを追加
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(console_handler)

def log_error(message, e):
    """エラーメッセージをログファイルに出力し、コンソールにも表示する"""
    error_message = f"{message}: {e}"
    logging.error(error_message)
    print(f"❌ ログ記録済みエラー: {error_message}")

# --- Google Drive 認証とファイル操作関数 ---

def upload_log_file(drive_service, folder_id):
    """生成されたログファイルをGoogle Driveにアップロード/更新する"""
    try:
        with open(LOG_FILE_NAME, 'r') as f:
            log_content = f.read()

        # 既存ログファイルを検索
        file_list = drive_service.ListFile({
            'q': f"'{folder_id}' in parents and title='{LOG_FILE_NAME}' and trashed=false"
        }).GetList()

        if file_list:
            file = file_list[0]
        else:
            file = drive_service.CreateFile({
                'title': LOG_FILE_NAME,
                'parents': [{'id': folder_id}],
                'mimeType': 'text/plain'
            })

        file.SetContentString(log_content)
        file.Upload()
        print(f"✅ ログファイル '{LOG_FILE_NAME}' をGoogle Driveにアップロード/更新しました。")

    except FileNotFoundError:
        print(f"⚠️ ログファイル '{LOG_FILE_NAME}' が見つかりません。アップロードをスキップします。")
    except Exception as e:
        log_error(f"致命的なエラー: ログファイルのGoogle Driveへのアップロードに失敗しました", e)

def authenticate_drive():
    """GitHub Actions環境でSecretsから生成された認証情報で認証を行う"""
    try:
        gauth = GoogleAuth()
        gauth.DEFAULT_SETTINGS['client_config_file'] = 'credentials.json' 
        gauth.LocalWebserverAuth = lambda: None 
        gauth.ServiceAuth()
        return GoogleDrive(gauth)
    except Exception as e:
        log_error("Google Driveの認証に失敗しました", e)
        return None

def download_csv_to_dataframe(drive_service, file_id):
    """ファイルIDを指定してCSVをダウンロードし、Pandas DataFrameとして返す"""
    try:
        file = drive_service.CreateFile({'id': file_id})
        downloaded_content = file.GetContentString(mimetype='text/csv')
        df = pd.read_csv(io.StringIO(downloaded_content), parse_dates=['date'])
        return df
    except Exception as e:
        log_error(f"ファイルID {file_id} のダウンロードまたは読み込みに失敗しました", e)
        return None

def download_existing_movie_csv(drive_service, folder_id, file_name):
    """既存の映画別ファイルをダウンロードしDataFrameとして返す（存在しない場合はNone）"""
    try:
        # ... (既存のファイル検索ロジックは変更なし)
        file_list = drive_service.ListFile({
            'q': f"'{folder_id}' in parents and title='{file_name}' and trashed=false"
        }).GetList()

        if file_list:
            file = file_list[0]
            downloaded_content = file.GetContentString(mimetype='text/csv')
            df = pd.read_csv(io.StringIO(downloaded_content), parse_dates=['日付'])
            print(f"既存ファイル {file_name} をダウンロードしました。")
            return df
        else:
            return None 
    except Exception as e:
        log_error(f"既存ファイル {file_name} のダウンロード中にエラーが発生しました", e)
        return None

def upload_dataframe_as_csv(drive_service, df, folder_id, file_name):
    """DataFrameをCSVとして指定フォルダにアップロード/更新する"""
    try:
        output = io.StringIO()
        df.to_csv(output, index=False)
        output_content = output.getvalue()

        # ... (既存ファイルの検索・作成ロジックは変更なし)
        file_list = drive_service.ListFile({
            'q': f"'{folder_id}' in parents and title='{file_name}' and trashed=false"
        }).GetList()

        if file_list:
            file = file_list[0]
        else:
            file = drive_service.CreateFile({
                'title': file_name,
                'parents': [{'id': folder_id}],
                'mimeType': 'text/csv'
            })

        file.SetContentString(output_content)
        file.Upload()
        print(f"✅ ファイルのアップロード/更新が完了しました: {file_name}")

    except Exception as e:
        log_error(f"{file_name} のアップロードに失敗しました", e)

def get_or_create_folder_id(drive_service, parent_folder_id, folder_name):
    """指定された親フォルダ内にフォルダを検索し、なければ作成してIDを返す"""
    try:
        # ... (フォルダ検索・作成ロジックは変更なし)
        query = f"title='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false"
        file_list = drive_service.ListFile({'q': query}).GetList()

        if file_list:
            folder_id = file_list[0]['id']
            print(f"既存のフォルダ '{folder_name}' を使用します。ID: {folder_id}")
            return folder_id
        else:
            folder_metadata = {
                'title': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [{'id': parent_folder_id}]
            }
            folder = drive_service.CreateFile(folder_metadata)
            folder.Upload()
            folder_id = folder['id']
            print(f"新規フォルダ '{folder_name}' を作成しました。ID: {folder_id}")
            return folder_id
    except Exception as e:
        log_error(f"フォルダ '{folder_name}' の取得または作成に失敗しました", e)
        return None


# --- メイン処理ロジック ---

def main_processor():
    # ログ設定を最初に実行
    setup_logging()
    
    drive_service = authenticate_drive()
    if drive_service is None:
        sys.exit(1) # 認証失敗時は即時終了（ログはauthenticate_driveで記録済み）

    # 1. データをダウンロード・読み込み
    df_youtube = download_csv_to_dataframe(drive_service, YOUTUBE_MASTER_FILE_ID)
    df_attendance = download_csv_to_dataframe(drive_service, ATTENDANCE_MASTER_FILE_ID)

    if df_youtube is None or df_attendance is None:
        # ダウンロード失敗時はログ記録済みのため、そのまま終了
        print("致命的なエラー: 入力データのダウンロードに失敗しました。処理を中断します。")
        upload_log_file(drive_service, FINAL_ANALYSIS_FOLDER_ID) # ログファイルをアップロード
        sys.exit(1)

    # 2. データの結合と必要な列の抽出/加工
    try:
        df_youtube = df_youtube[['date', 'movie_title', YOUTUBE_RANKING_COLUMN, 'daily_new_views']]
        df_youtube = df_youtube.rename(columns={'daily_new_views': '動画の再生数'})
        
        combined_df = pd.merge(
            df_youtube,
            df_attendance,
            on=['date', 'movie_title'],
            how='inner'
        )
        
        if len(combined_df) == 0:
            print("警告: 結合レコードが0件です。日次のデータが両方に存在しませんでした。")
            return
        
        # ... (以降のデータ加工ロジックは変更なし)
        final_combined_df = combined_df[[
            'movie_title', 
            'daily_sales_or_attendance', 
            '動画の再生数', 
            'date',
            YOUTUBE_RANKING_COLUMN 
        ]].rename(columns={
            'movie_title': '映画名',
            'daily_sales_or_attendance': '興行収入_動員数',
            'date': '日付',
            YOUTUBE_RANKING_COLUMN: 'ナンバリング'
        })
        
        final_combined_df['ナンバリング'] = final_combined_df['ナンバリング'].astype(str).str.replace(r'\..*$', '', regex=True).astype(int)

        print(f"✅ マスターデータの結合と加工が完了しました。総レコード数: {len(final_combined_df)}")

    except Exception as e:
        log_error("データ結合/加工フェーズで致命的なエラーが発生しました", e)
        upload_log_file(drive_service, FINAL_ANALYSIS_FOLDER_ID)
        sys.exit(1)

    # 3. ナンバリングフォルダの作成とファイル更新
    ranking_ids = sorted(final_combined_df['ナンバリング'].unique())

    for ranking_id in ranking_ids:
        # フォルダ操作はget_or_create_folder_id内でロギングされる
        ranking_folder_id = get_or_create_folder_id(
            drive_service, 
            FINAL_ANALYSIS_FOLDER_ID, 
            str(ranking_id)
        )
        
        if ranking_folder_id is None:
             continue
        
        df_ranking_data = final_combined_df[final_combined_df['ナンバリング'] == ranking_id].copy()

        for movie in df_ranking_data['映画名'].unique():
            df_new_data = df_ranking_data[df_ranking_data['映画名'] == movie].copy()
            output_file_name = f"{movie}.csv"
            
            # ダウンロード操作はdownload_existing_movie_csv内でロギングされる
            df_existing = download_existing_movie_csv(drive_service, ranking_folder_id, output_file_name)
            
            try:
                if df_existing is not None and not df_existing.empty:
                    df_updated = pd.concat([df_existing, df_new_data], ignore_index=True)
                    df_updated.drop_duplicates(subset=['映画名', '日付'], keep='last', inplace=True)
                    print(f"📈 {output_file_name} を更新しました。レコード数: {len(df_updated)}")
                else:
                    df_updated = df_new_data
                    print(f"🚀 {output_file_name} を新規作成します。")

            except Exception as e:
                log_error(f"映画 '{movie}' のデータ結合ロジックでエラーが発生しました", e)
                continue # この映画の処理をスキップし、次の映画に進む

            # アップロード操作はupload_dataframe_as_csv内でロギングされる
            df_updated = df_updated.sort_values(by=['ナンバリング', '日付'])
            upload_dataframe_as_csv(
                drive_service, 
                df_updated, 
                ranking_folder_id, 
                output_file_name
            )
        
    print("\n✅ 全ナンバリングフォルダへのファイル更新が完了しました。")
    
    # 最後にログファイルをアップロード
    upload_log_file(drive_service, FINAL_ANALYSIS_FOLDER_ID)


if __name__ == '__main__':
    main_processor()
