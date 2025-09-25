import os
import json
import gspread
import pandas as pd
import google.generativeai as genai
import re
import time
import threading
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, abort, jsonify
from flask_cors import CORS
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    PushMessageRequest,
    ReplyMessageRequest,
    TextMessage,
    FlexMessage,
    FlexContainer
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

app = Flask(__name__)
CORS(app)

# --- LIFF ID定義 ---
SCHEDULE_LIFF_ID = "2008066763-X5mxymoj"
QUESTIONNAIRE_LIFF_ID = "2008066763-JAkGQkmw"

# --- 認証設定 ---
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds_path = '/etc/secrets/google_credentials.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
client = gspread.authorize(creds)

spreadsheet = client.open("店舗マスタ_LUMINA Offer用")
user_management_sheet = spreadsheet.worksheet("ユーザー管理")
offer_management_sheet = spreadsheet.worksheet("オファー管理")
# ★変更点②：新しい統合シートを読み込むように変更
salon_master_sheet = spreadsheet.worksheet("店舗マスタ")

# LINE API
configuration = Configuration(access_token=os.environ.get('YOUR_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.environ.get('YOUR_CHANNEL_SECRET'))

# Gemini API
genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))
model = genai.GenerativeModel('gemini-1.5-flash-latest')

# ★変更点①：即時オファー送信用の関数
def process_and_send_offer(user_id, user_wishes):
    try:
        ranked_ids, matched_salon, offer_text = find_and_generate_offer(user_wishes)
        
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)

            if matched_salon:
                today_str = datetime.today().strftime('%Y/%m/%d')
                
                offer_headers = ['ユーザーID', '店舗ID', 'オファー送信日', 'オファー状況']
                initial_offer_data = {
                    "ユーザーID": user_id,
                    "店舗ID": matched_salon.get('店舗ID'),
                    "オファー送信日": today_str,
                    "オファー状況": "送信済み"
                }
                new_offer_row = [initial_offer_data.get(h, '') for h in offer_headers]
                offer_management_sheet.append_row(new_offer_row, value_input_option='USER_ENTERED')
            
                flex_container = FlexContainer.from_dict(create_salon_flex_message(matched_salon, offer_text))
                messages = [FlexMessage(alt_text=f"{matched_salon['店舗名']}からのオファー", contents=flex_container)]
                line_bot_api.push_message(PushMessageRequest(to=user_id, messages=messages))
            # マッチしなかった場合は何もしない（サイレント）

    except Exception as e:
        print(f"オファー送信中のエラー: {e}")

# ★変更点②：データベース統合と新マッチングロジックに対応
def find_and_generate_offer(user_wishes):
    # 統合された新しい「店舗マスタ」シートのみを読み込む
    all_salons_data = salon_master_sheet.get_all_records()

    if not all_salons_data:
        return None, None, "サロン情報が見つかりません。"

    salons_df = pd.DataFrame(all_salons_data)
    
    # --- 新しいマッチングロジック ---
    user_role = user_wishes.get("role")
    user_license = user_wishes.get("license") # フォームから受け取った免許情報

    # 1. 募集状況が「募集中」の求人のみに絞り込む
    active_salons = salons_df[salons_df['募集状況'] == '募集中'].copy()
    
    # 2. ユーザーの役職に合う求人のみに絞り込む
    role_matched_salons = active_salons[active_salons['役職'] == user_role]
    
    # 3. 美容師免許の要件に合う求人のみに絞り込む
    # ユーザーが「取得済み」の場合、「取得」を必須とする求人のみ
    if user_license == "取得済み":
        license_matched_salons = role_matched_salons[role_matched_salons['美容師免許'] == '取得']
    else: # ユーザーが「未取得」の場合、「未取得」でもOKな求人も含める
        license_matched_salons = role_matched_salons[role_matched_salons['美容師免許'].isin(['取得', '未取得'])]

    if license_matched_salons.empty:
        return None, None, "条件に合う募集中の求人が見つかりませんでした。"
    
    salons_to_consider_df = license_matched_salons
    # --- マッチングロジックここまで ---

    salons_json_string = salons_to_consider_df.to_json(orient='records', force_ascii=False)

    prompt = f"""
    あなたは、美容師向けのスカウトサービス「LUMINA Offer」の優秀なAIアシスタントです。
    # 候補者プロフィール:
    {json.dumps(user_wishes, ensure_ascii=False)}
    # 候補となる求人リスト:
    {salons_json_string}
    # あなたのタスク:
    1. **スコアリング**: 以下の基準で各求人を評価し、合計スコアが高い順に最大3件まで選んでください。
       - 候補者が「最も興味のある待遇」を、求人が提供している('待遇'に含まれている)場合: +10点
       - 候補者のMBTIの性格特性が、求人の「特徴」や「待遇」と相性が良い場合: +5点
    2. **オファー文章生成**: スコアが最も高かった1件目のサロンについてのみ、以下のルールを厳守し、候補者がカジュアル面談に行きたくなるようなオファー文章を150字以内で作成してください。
       - 文章の冒頭は必ず「LUMINA Offerから、あなたに特別なオファーが届いています。」で始めてください。
       - 候補者が「最も興味のある待遇」が、なぜそのサロンで満たされるのかを説明すること。
       - 候補者のMBTIの性格特性が、どのようにそのサロンの文化や特徴と合致するのかを説明すること。
       - 文章の最後は、必ず「まずは、サロンから話を聞いてみませんか？」という一文で締めてください。
       - 禁止事項: サロンが直接オファーを送っていると候補者が錯覚するような表現は避けてください。あくまで「LUMINA Offer」というサービスからの推薦・オファーであることを明確にしてください。過度に堅苦しくない、自然な言葉遣いを心がけてください。
    # 回答フォーマット:
    以下のJSON形式で、厳密に回答してください。
    {{
      "ranked_store_ids": [ (ここにスコア上位の'店舗ID'を数値のリストで記述。例: [101, 108, 125]) ],
      "first_offer_message": "(ここに1件目のサロン用のオファー文章を記述)"
    }}
    """
    
    response = model.generate_content(prompt)
    
    try:
        response_text = response.text
        json_str_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if not json_str_match: raise ValueError("Response does not contain a valid JSON object.")
        json_str = json_str_match.group(0)
        gemini_response = json.loads(json_str)
        
        ranked_ids = gemini_response.get("ranked_store_ids")
        first_offer_message = gemini_response.get("first_offer_message")
        
        if not ranked_ids: return None, None, "最適なサロンが見つかりませんでした。"
            
        first_match_id = ranked_ids[0]
        matched_salon_info_series = salons_to_consider_df[salons_to_consider_df['店舗ID'] == first_match_id]
        if matched_salon_info_series.empty: return None, None, "マッチしたサロン情報が見つかりませんでした。"
        
        matched_salon_info = matched_salon_info_series.iloc[0].to_dict()
        
        return ranked_ids, matched_salon_info, first_offer_message
    except Exception as e:
        print(f"Geminiからの応答解析エラー: {e}")
        print(f"Geminiからの元テキスト: {response.text}")
        return None, None, "最適なサロンが見つかりませんでした。"

# (create_salon_flex_message, get_age_from_birthdate, callback, handle_message は変更なし)
# ...

@app.route("/trigger-offer", methods=['POST'])
def trigger_offer():
    data = request.get_json()
    if not data: return jsonify({"status": "error", "message": "No data provided"}), 400
    user_id = data.get('userId')
    user_wishes = data.get('wishes')
    if not user_id or not user_wishes: return jsonify({"status": "error", "message": "Missing userId or wishes"}), 400
    
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            welcome_message = (
                "ご登録いただき、誠にありがとうございます！\n"
                "LUMINA Offerが、あなたにプロフィールを拝見してピッタリな『好待遇サロンの公認オファー』を、このLINEアカウントを通じてご連絡いたします。\n"
                "楽しみにお待ちください！"
            )
            line_bot_api.push_message(PushMessageRequest(
                to=user_id,
                messages=[TextMessage(text=welcome_message)]
            ))
    except Exception as e:
        print(f"ウェルカムメッセージの送信エラー: {e}")

    if 'birthdate' in user_wishes and user_wishes['birthdate']:
        age = get_age_from_birthdate(user_wishes['age'])
        user_wishes['age'] = f"{ (age // 10) * 10 }代"

    try:
        # ★変更点：新しい「ユーザー管理」シートのヘッダーに対応
        user_headers = user_management_sheet.row_values(1)
        user_row_dict = {
            "ユーザーID": user_id,
            "登録日": datetime.today().strftime('%Y/%m/%d'),
            "ステータス": 'オファー中',
            "氏名": user_wishes.get('full_name'),
            "性別": user_wishes.get('gender'),
            "生年月日": user_wishes.get('birthdate'),
            "電話番号": user_wishes.get('phone_number'),
            "美容師免許": user_wishes.get('license'), # ★追加
            "MBTI": user_wishes.get('mbti'),
            "役職": user_wishes.get('role'),
            "希望勤務地": user_wishes.get('area'),
            "職場満足度": user_wishes.get('satisfaction'),
            "興味のある待遇": user_wishes.get('perk'),
            "現在の状況": user_wishes.get('current_status'),
            "転職希望時期": user_wishes.get('timing')
        }
        user_row = [user_row_dict.get(h, '') for h in user_headers if h not in ['Q1_職場改善点', 'Q2_重視点', 'Q3_理想の美容師像', 'Q4_得意な技術', 'Q5_苦手な技術', 'Q6_好きな客層', 'Q7_目指す年収', 'Q8_キャリアプラン', 'Q9_その他']]
        
        cell = user_management_sheet.find(user_id, in_column=1)
        if cell:
            range_to_update = f'A{cell.row}:{chr(ord("A") + len(user_row) - 1)}{cell.row}'
            user_management_sheet.update(range_to_update, [user_row])
        else:
            user_management_sheet.append_row(user_row)
    except Exception as e:
        print(f"ユーザー管理シートへの書き込みエラー: {e}")

    # ★変更点①：遅延させずに、即座にオファー送信処理を実行
    process_and_send_offer(user_id, user_wishes)
    
    return jsonify({"status": "success", "message": "Offer task processed immediately"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)
# (その他の関数は、前回の最終版から変更ありません)
# ...