import pandas as pd
import io
import numpy as np

# --- Google Drive ファイルID 設定 ---
# 担当者から共有されたIDを埋め込み済み
YOUTUBE_MASTER_FILE_ID = '1fZntJuEUcpTeXcbR5aGWvVj8WfsAE_Cb'
ATTENDANCE_MASTER_FILE_ID = '1EJeJ5215gngU_7YxlFnj_3kFA4q--Koe'
FINAL_ANALYSIS_FOLDER_ID = '1w8pmCqX2TxJHfearx1XBmm4XRAXsV8We'

# ⚠️ ナンバリングの列名を設定（ユーザーの指定により 'no' を使用）
YOUTUBE_RANKING_COLUMN = 'no' 

# Google Drive操作のためにPyDrive2と認証関連のライブラリをインポート
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# --- Google Drive 認証とファイル操作関数 ---

def authenticate_drive():
    """GitHub Actions環境でSecretsから生成された認証情報で認証を行う"""
    gauth = GoogleAuth()
    gauth.DEFAULT_SETTINGS['client_config_file'] = 'credentials.json' 
    gauth.LocalWebserverAuth = lambda: None 
    gauth.ServiceAuth()
    return GoogleDrive(gauth)

def download_csv_to_dataframe(drive_service, file_id):
    """ファイルIDを指定してCSVをダウンロードし、Pandas DataFrameとして返す"""
    try:
        file = drive_service.CreateFile({'id': file_id})
        downloaded_content = file.GetContentString(mimetype='text/csv')
        df = pd.read_csv(io.StringIO(downloaded_content), parse_dates=['date'])
        return df
    except Exception as e:
        print(f"エラー: ファイルID {file_id} のダウンロードまたは読み込みに失敗しました: {e}")
        return None

def download_existing_movie_csv(drive_service, folder_id, file_name):
    """既存の映画別ファイルをダウンロードしDataFrameとして返す（存在しない場合はNone）"""
    try:
        # フォルダ内のファイル名で検索
        file_list = drive_service.ListFile({
            'q': f"'{folder_id}' in parents and title='{file_name}' and trashed=false"
        }).GetList()

        if file_list:
            file = file_list[0]
            downloaded_content = file.GetContentString(mimetype='text/csv')
            # 既存ファイルの'日付'カラムを日付型として読み込みます
            df = pd.read_csv(io.StringIO(downloaded_content), parse_dates=['日付'])
            print(f"既存ファイル {file_name} をダウンロードしました。")
            return df
        else:
            return None 
    except Exception as e:
        print(f"警告: 既存ファイル {file_name} のダウンロード中にエラーが発生しました: {e}")
        return None

def upload_dataframe_as_csv(drive_service, df, folder_id, file_name):
    """DataFrameをCSVとして指定フォルダにアップロード/更新する"""
    try:
        output = io.StringIO()
        df.to_csv(output, index=False)
        output_content = output.getvalue()

        # 既存ファイルの検索
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
        print(f"エラー: {file_name} のアップロードに失敗しました: {e}")

def get_or_create_folder_id(drive_service, parent_folder_id, folder_name):
    """指定された親フォルダ内にフォルダを検索し、なければ作成してIDを返す"""
    # フォルダの検索クエリ
    query = f"title='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false"
    file_list = drive_service.ListFile({'q': query}).GetList()

    if file_list:
        folder_id = file_list[0]['id']
        print(f"既存のフォルダ '{folder_name}' を使用します。ID: {folder_id}")
        return folder_id
    else:
        # フォルダの作成
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


# --- メイン処理ロジック ---

def main_processor():
    drive_service = authenticate_drive()
    if drive_service is None:
        print("致命的なエラー: Google Driveの認証に失敗しました。")
        return

    # 1. データをダウンロード・読み込み
    df_youtube = download_csv_to_dataframe(drive_service, YOUTUBE_MASTER_FILE_ID)
    df_attendance = download_csv_to_dataframe(drive_service, ATTENDANCE_MASTER_FILE_ID)

    if df_youtube is None or df_attendance is None:
        print("致命的なエラー: 入力データのダウンロードに失敗しました。処理を中断します。")
        return

    # 2. データの結合と必要な列の抽出/加工
    
    # 'no'列を含めてYouTubeデータから抽出
    df_youtube = df_youtube[['date', 'movie_title', YOUTUBE_RANKING_COLUMN, 'daily_new_views']]
    df_youtube = df_youtube.rename(columns={'daily_new_views': '動画の再生数'})
    
    # 'date'と'movie_title'をキーにinner join（Bデータから映画情報を検索・抽出）
    combined_df = pd.merge(
        df_youtube,
        df_attendance,
        on=['date', 'movie_title'],
        how='inner'
    )
    
    if len(combined_df) == 0:
        print("警告: 結合レコードが0件です。日次のデータが両方に存在しませんでした。")
        return

    # 最終的な出力列を選択し、列名を日本語に統一
    final_combined_df = combined_df[[
        'movie_title', 
        'daily_sales_or_attendance', 
        '動画の再生数', 
        'date',
        YOUTUBE_RANKING_COLUMN # 'no'列を含める
    ]].rename(columns={
        'movie_title': '映画名',
        'daily_sales_or_attendance': '興行収入_動員数',
        'date': '日付',
        YOUTUBE_RANKING_COLUMN: 'ナンバリング' # 出力ファイルでは日本語名 'ナンバリング' を使用
    })
    
    # 'ナンバリング'列を整数型に変換（フォルダ名として使用するため、小数点以下を削除）
    final_combined_df['ナンバリング'] = final_combined_df['ナンバリング'].astype(str).str.replace(r'\..*$', '', regex=True).astype(int)

    print(f"✅ マスターデータの結合と加工が完了しました。総レコード数: {len(final_combined_df)}")

    # 3. ナンバリングフォルダの作成とファイル更新（2段階のループ）
    
    ranking_ids = sorted(final_combined_df['ナンバリング'].unique())

    for ranking_id in ranking_ids:
        # 3-1. ナンバリングフォルダのIDを取得 (存在しなければ作成)
        ranking_folder_id = get_or_create_folder_id(
            drive_service, 
            FINAL_ANALYSIS_FOLDER_ID, 
            str(ranking_id) # フォルダ名は文字列
        )
        
        # ナンバリングで絞り込んだデータ
        df_ranking_data = final_combined_df[final_combined_df['ナンバリング'] == ranking_id].copy()

        # 3-2. ナンバリング内のデータをさらに映画名でループ
        movie_titles_in_ranking = df_ranking_data['映画名'].unique()
        
        for movie in movie_titles_in_ranking:
            # 映画ごとの新しい日次データ
            df_new_data = df_ranking_data[df_ranking_data['映画名'] == movie].copy()
            
            # ファイル名の定義 (例: '映画A.csv')
            output_file_name = f"{movie}.csv"
            
            # 既存の映画別ファイルをダウンロード (ナンバリングフォルダからダウンロード)
            df_existing = download_existing_movie_csv(drive_service, ranking_folder_id, output_file_name)
            
            if df_existing is not None and not df_existing.empty:
                # 既存データと新しい日次データを結合
                df_updated = pd.concat([df_existing, df_new_data], ignore_index=True)
                
                # 重複行の削除（日付と映画名で上書き）
                df_updated.drop_duplicates(
                    subset=['映画名', '日付'], 
                    keep='last', 
                    inplace=True
                )
                print(f"📈 {output_file_name} を更新しました。レコード数: {len(df_updated)}")
            else:
                df_updated = df_new_data
                print(f"🚀 {output_file_name} を新規作成します。")

            # 4. Google Driveのナンバリングフォルダにアップロード
            upload_dataframe_as_csv(
                drive_service, 
                df_updated, 
                ranking_folder_id, 
                output_file_name
            )
        
    print("\n✅ 全ナンバリングフォルダへのファイル更新が完了しました。")


if __name__ == '__main__':
    main_processor()
