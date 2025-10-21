# ワークフローの名称
name: Project Work Data Scraping

# トリガー条件
on:
  # 手動実行を許可
  workflow_dispatch:
  # 定時実行 (毎日 日本時間 午前0時15分)
  schedule:
    # UTCで15時15分 = JSTで0時15分
    - cron: '15 15 * * *'

jobs:
  scrape-and-upload:
    # 実行環境の指定
    runs-on: ubuntu-latest
    
    # 環境変数の設定 (Google Driveのサービスアカウント認証情報)
    env:
      GDRIVE_SA_KEY: ${{ secrets.GDRIVE_SA_KEY }} # GitHub Secretsに登録済みの鍵名

    steps:
      # 1. ソースコードをチェックアウト
      - name: Checkout repository
        uses: actions/checkout@v4

      # 2. Python環境をセットアップ
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      # 3. 依存関係のインストール
      - name: Install dependencies
        run: |
          # 必要なライブラリをインストール（requirements.txtが必要です）
          pip install -r requirements.txt

      # 4. Google Driveサービスアカウント認証情報をファイルに出力
      - name: Create credentials.json
        run: echo "$GDRIVE_SA_KEY" > credentials.json
        # 実行完了後、ファイルを削除する設定
        # secretsを使っているため、このファイルはGitには追跡されず、runnerからも自動的に削除されますが、明示的に書き出すのはこのステップです

      # 5. メインのPythonスクリプトを実行
      - name: Run main_c.py
        run: python main_c.py
