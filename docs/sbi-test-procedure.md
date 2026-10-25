# SBIのテスト手順

finance_clientのルートディレクトリで、これまで使用していたPython環境を有効にして実行します。以下の引用符は半角の二重引用符です。
コマンドプロンプト（cmd.exe）では、一重引用符も検索パターンに含まれるため使わないでください。

## 1. 模擬画面で変更追従を検証

```powershell
python -m unittest discover -s tests/offline -p "sbi_*_test.py" -v
```

165件が成功し、最後に `OK` が出れば合格です。
Chromeを使いますが、SBIへの接続・実注文・Gmail取得・実AI APIへの問い合わせはしません。
AI応答はモックです。変更後の画面で発注→未約定照会→取消→空一覧の確認も含みます。

この流れだけ実行する場合:

```powershell
python -m unittest discover -s tests/offline -p "sbi_selenium_recovery_test.py" -k offline_order_inquiry_cancel_lifecycle -v
```

## 2. finance_clientの設定と実接続を確認

`finance_client\.env` で、以下を有効にします。
既存の別ファイルを使う場合、注文テストには `--env-file` でその場所を指定できます。

```dotenv
SBI_AI_HEALING=1
SBI_AI_OPERATION_HEALING=1
```

普段のChromeに接続する場合（Chrome 144以降）:

```dotenv
SBI_CHROME_DEBUGGER_ADDRESS=auto
```

普段のChromeで `chrome://inspect/#remote-debugging` を開いてリモートデバッグを有効にし、
スクリプトの接続時にChromeが求める接続許可を承認します。ブラウザーの再起動やプロファイルのコピーは行いません。
この許可はChromeの仕様により接続のたびに必要です。確認を不要にする固定プロファイル方式は [sbi-ai-recovery.md](sbi-ai-recovery.md#普段のchromeを使用する) を参照してください。
Chromeの許可式接続とSeleniumの接続方式の違いは、同じPC内で動く一時的な接続処理が吸収します。追加の設定は不要です。
Seleniumは既存のChromeに新しいSBI用タブを開き、終了時はそのタブと接続だけを閉じます。
普段のChromeの他のタブ・プロファイル設定は維持します。UAの固定や画像の無効化も行いません。
同じChromeを使っても、SBIの追加認証が必ず省略されるわけではありません。
Chrome側の接続機能が無効な場合は、別のChromeを勝手に起動せず、設定案内を表示して停止します。
Chromeの標準データ保存先以外を使っている場合は `SBI_CHROME_ATTACH_USER_DATA_DIR` で指定できます。
Chromeの[既存ブラウザーへの接続の説明](https://developer.chrome.com/blog/chrome-devtools-mcp-debug-your-browser-session)も参照してください。

毎回のデバッグ接続許可を求めない運用では、`SBI_CHROME_DEBUGGER_ADDRESS` を空にして、以前からあるSBI用の固定プロファイルを再利用します。
設定例と通常のChromeとの違いは [固定プロファイル方式](sbi-ai-recovery.md#接続許可を毎回求めない固定プロファイル方式) を参照してください。

既存の `SBI_AI_MODEL`、`OPENAI_API_KEY`、`sbi_id`、`sbi_password`、Gmail認可設定を使います。
実口座の発注・取消には `sbi_trade_password` も必要です。

```cmd
cd /d "C:\path\to\finance_client"
python samples/sbi_read_only.py
```

買付余力・保有銘柄・注文一覧が取得できれば合格です。これは読み取り専用です。
finance_client側のこのサンプルは `--connect` なしで実口座の照会を行い、取得した口座情報を出力します。

## 3. 実口座で指値発注→未約定照会→取消を一括確認

発注せずに実画面の入力までを確認する場合は `--prepare-only` を使います。
注文一覧の照会、注文画面への移動、数量・指値・取引パスワードの入力、
入力値の保持と発注ボタンの表示・有効状態を確認して終了します。最終ボタンはクリックしません。

```cmd
python samples/sbi_order_cycle.py --env-file "..\trade_strategy\.env" --symbol CODE --units UNITS --price PRICE --prepare-only
```

成功時は `order_form_verified: true`、`submission_button_ready: true`、`order_submitted: false` を出力します。
この確認は、発注後に実施されるSBI側の受付・余力等の検証が通ることまでは保証しません。
発注ボタンの後に表示される電話の追加認証も、`--prepare-only` では実画面の確認対象になりません。
電話認証の分岐はオフラインテストで、発注／取消後の認証待ち、期限切れ、認証後の結果確認、再送防止を検証します。

`--prepare-only --exercise-recovery` とすると、数量・指値・取引パスワード・確認省略・発注ボタンの
既存セレクターが見つからない状態を作り、復旧経路で同じ確認を行います。SBIのHTMLは変更しません。
この追加オプションは `--prepare-only` との組み合わせでのみ使用できます。
診断ログには対象5項目の検索失敗と、復旧の調査に使う公開ラベル・タグ・構造パスも記録します。

`samples/sbi_order_cycle.py` は、指定した銘柄に実際の指値注文を送信します。
未約定一覧で対象を1件確認できた場合だけ取消へ進み、取消完了表示と一覧から消えたことを確認します。
銘柄・単位数・指値は利用者が決めます。`--units` は株数ではなく売買単位数で、
実際の株数は「単位数 × 画面に表示された売買単位」です。

`CODE`、`UNITS`、`PRICE` をご自身で選んだ値に置き換えます。変数や対話入力は不要です。
同じコマンドを保存して再実行できます。

設定確認だけ（ブラウザー・注文・AI問い合わせは開始しません）:

```cmd
python samples/sbi_order_cycle.py --symbol CODE --units UNITS --price PRICE
```

実口座で一連の処理を実行:

```cmd
python samples/sbi_order_cycle.py --symbol CODE --units UNITS --price PRICE --execute
```

取引時に電話の追加認証が表示された場合、ブラウザーを閉じずに同じ実行で待機します。
SBIに登録済みの電話から、その時点の画面に表示されている認証用電話番号へ発信してください。
発信後、ブラウザーの「認証する」を手動で押して完了した場合は自動で検出し、端末のEnterなしで元の注文／取消の結果確認に戻ります。
従来どおり端末でEnterを押し、スクリプトに「認証する」を1回押させる方法も使えます。
電話発信そのものは自動化していません。認証が失敗・期限切れの場合、発注・取消や認証ボタンを繰り返し送信しません。
待機時間は `.env` の `SBI_PHONE_AUTH_TIMEOUT_SECONDS` で設定でき、既定／上限は180秒です。
認証ボタン押下後も、別の20秒制限は設けず、残りの待機時間を使います。SBI画面が期限切れになればその時点で停止します。
SBI側の認証期限は3分であり、この設定によって期限を延長することはできません。
電話番号や電話番号を含む画面本文はAIや診断ログに送らず、認証待ち・完了・失敗の区分を記録します。
SBIの[取引時認証の説明](https://www.sbisec.co.jp/ETGate/?OutSide=on&burl=search_home&cat1=home&cat2=none&dir=service&file=home_security_adaptive_authentication.html&getFlg=on)も参照してください。

既存の `trade_strategy\.env` を再利用する場合（設定ファイルを複製する必要はありません）:

```cmd
python samples/sbi_order_cycle.py --env-file "..\trade_strategy\.env" --symbol CODE --units UNITS --price PRICE --execute
```

成功時の出力:

実行開始時に、`finance_client/logs/sbi_order_cycle_日時_PID.jsonl` の保存先を表示します。
この診断ログには処理段階、例外の種類とコード上の位置、Selenium要素・照会レシピのAI要求／応答を記録します。
送信結果の判定時には、受付・拒否・判定不能の区分と、登録済みの公開ラベル・画面構造のハッシュも記録します。
パスワード・認証コード・銘柄・数量・価格・AIへの入力や返答本文・例外メッセージ全文は保存しません。
`ai_response` はAPIから返答を受け取った記録であり、その後の検証や取引の成功を表すものではありません。

```json
{"order_accepted": true, "open_order_verified": true, "cancellation_confirmed": true, "remaining_matching_orders": 0}
```

- 対象銘柄に既存の未約定注文がある場合は、発注前に停止します。
- 発注後に同じ銘柄の注文が複数ある場合は、取消せず停止します。
- 現行の照会結果には注文番号がないため、対象銘柄の別注文を並行して出さない状態で実施します。
- 約定した場合など、未約定を確認できない場合は取消へ進みません。
- 発注・取消は各1回です。再試行するのは一覧の読み取りだけです。
- 途中で停止した場合は、SBIの注文状況を確認してから再実行します。異常終了時に自動再実行する設定にはしません。

このサンプルの実口座実行は未検証です。制御フローは以下のモックテストで確認できます。

```powershell
python -m unittest discover -s tests/offline -p "sbi_order_cycle_test.py" -v
```

通常の操作が成功しただけでは、失敗後のAI復旧が発火した証明にはなりません。
画面変更を意図的に入れた復旧の確認には手順1を使用します。
実口座の画面を意図的に書き換えて取引操作を進める手順は含めていません。

既存の `tests/order_methods/sbi_client.test.py` は固定銘柄・古い呼び出しを含むため、この手順では使用しません。
