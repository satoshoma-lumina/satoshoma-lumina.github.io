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
# ★変更点：PostbackEventが不要になったため、インポートから削除
from linebot.v3.webhooks import MessageEvent, TextMessageContent

app = Flask(__name__)
CORS(app)

# --- ★変更点：LIFF IDをファイルの先頭で定数として定義 ---
# 面談日程調整フォームのLIFF ID
SCHEDULE_LIFF_ID = "2008066763-X5mxymoj"

# --- 認証設定 ---
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds_path = '/etc/secrets/google_credentials.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
client = gspread.authorize(creds)

spreadsheet = client.open("店舗マスタ_LUMINA Offer用")
user_management_sheet = spreadsheet.worksheet("ユーザー管理")
offer_management_sheet = spreadsheet.worksheet("オファー管理")
store_master_sheet = spreadsheet.worksheet("店舗マスタ")
postings_sheet = spreadsheet.worksheet("募集求人")

# LINE API
configuration = Configuration(access_token=os.environ.get('YOUR_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.environ.get('YOUR_CHANNEL_SECRET'))

# Gemini API
genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))
model = genai.GenerativeModel('gemini-1.5-flash-latest')

def send_delayed_offer(user_id, user_wishes):
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

    except Exception as e:
        print(f"遅延送信中のエラー: {e}")

def find_and_generate_offer(user_wishes):
    # (この関数に変更はありません)
    store_master_data = store_master_sheet.get_all_records()
    postings_data = postings_sheet.get_all_records()

    if not store_master_data or not postings_data:
        return None, None, "サロン情報が見つかりません。"

    stores_df = pd.DataFrame(store_master_data)
    postings_df = pd.DataFrame(postings_data)
    
    user_role = user_wishes.get("role")
    if not user_role: return None, None, "役職情報がありません。"
    active_postings = postings_df[(postings_df['募集状況'] == '募集中') & (postings_df['役職'] == user_role)]
    if active_postings.empty: return None, None, "ご希望の役職に合う募集中の求人が見つかりませんでした。"

    salons_to_consider_df = pd.merge(active_postings, stores_df, how='left', on='店舗ID')
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

def create_salon_flex_message(salon, offer_text):
    role = salon.get("role_x") or salon.get("役職")
    salon_id = salon.get('店舗ID')
    # ★変更点：ボタンを押したら直接LIFFが開くように、アクションを「uri」に変更
    liff_url = f"https://liff.line.me/{SCHEDULE_LIFF_ID}?salonId={salon_id}"
    
    return {
        "type": "bubble", "hero": { "type": "image", "url": salon.get("画像URL", ""), "size": "full", "aspectRatio": "20:13", "aspectMode": "cover" },
        "body": { "type": "box", "layout": "vertical", "contents": [ { "type": "text", "text": salon.get("店舗名", ""), "weight": "bold", "size": "xl" }, { "type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [ { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [ { "type": "text", "text": "勤務地", "color": "#aaaaaa", "size": "sm", "flex": 2 }, { "type": "text", "text": salon.get("住所", ""), "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]}, { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [ { "type": "text", "text": "募集役職", "color": "#aaaaaa", "size": "sm", "flex": 2 }, { "type": "text", "text": role, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]}, { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [ { "type": "text", "text": "メッセージ", "color": "#aaaaaa", "size": "sm", "flex": 2 }, { "type": "text", "text": offer_text, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]} ]} ] },
        "footer": { "type": "box", "layout": "vertical", "spacing": "sm", "contents": [ 
            { "type": "button", "style": "link", "height": "sm", "action": { "type": "uri", "label": "詳しく見る", "uri": "https://example.com" }}, 
            # ★変更点：アクションタイプを'uri'に、uriプロパティにLIFF URLを設定
            { "type": "button", "style": "primary", "height": "sm", "action": { "type": "uri", "label": "サロンから話を聞いてみる", "uri": liff_url }, "color": "#FF6B6B"} 
        ], "flex": 0 }
    }

def get_age_from_birthdate(birthdate):
    # (この関数に変更はありません)
    today = datetime.today()
    birth_date = datetime.strptime(birthdate, '%Y-%m-%d')
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

@app.route("/callback", methods=['POST'])
def callback():
    # (この関数に変更はありません)
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try: handler.handle(body, signature)
    except InvalidSignatureError: abort(400)
    return 'OK'

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    # (この関数に変更はありません)
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text="ご登録ありがとうございます。リッチメニューからプロフィールをご入力ください。")])
        )

# ★★★★★ 削除された機能 ★★★★★
# Postbackイベントを処理する handle_postback 関数は不要になったため削除しました。
# ★★★★★★★★★★★★★★★★★

@app.route("/submit-schedule", methods=['POST'])
def submit_schedule():
    # (この関数に変更はありません)
    data = request.get_json()
    user_id = data.get('userId')
    salon_id = data.get('salonId')
    
    try:
        all_records = offer_management_sheet.get_all_records()
        row_to_update = -1
        for i, record in enumerate(all_records):
            if record.get('ユーザーID') == user_id and str(record.get('店舗ID')) == str(salon_id):
                row_to_update = i + 2
                break
        
        if row_to_update != -1:
            update_values = [
                '日程調整中',
                data['interviewMethod'],
                data['date1'], data['startTime1'], data['endTime1'],
                data['date2'], data['startTime2'], data['endTime2'],
                data['date3'], data['startTime3'], data['endTime3']
            ]
            offer_management_sheet.update(f'D{row_to_update}:N{row_to_update}', [update_values])
            return jsonify({"status": "success", "message": "Schedule submitted successfully"})
        else:
            print(f"該当のオファーが見つかりません。UserID: {user_id}, SalonID: {salon_id}")
            return jsonify({"status": "error", "message": "Offer not found"}), 404

    except Exception as e:
        print(f"スプレッドシート更新エラー: {e}")
        return jsonify({"status": "error", "message": "Failed to update spreadsheet"}), 500

@app.route("/trigger-offer", methods=['POST'])
def trigger_offer():
    # (この関数に変更はありません)
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
        age = get_age_from_birthdate(user_wishes['birthdate'])
        user_wishes['age'] = f"{ (age // 10) * 10 }代"

    try:
        user_headers = user_management_sheet.row_values(1)
        user_row_dict = dict(zip(user_headers, [
            user_id, datetime.today().strftime('%Y/%m/%d'), 'オファー中', user_wishes.get('full_name'), 
            user_wishes.get('gender'), user_wishes.get('birthdate'), user_wishes.get('phone_number'), 
            user_wishes.get('mbti'), user_wishes.get('role'), user_wishes.get('area'), 
            user_wishes.get('satisfaction'), user_wishes.get('perk'), user_wishes.get('current_status'), 
            user_wishes.get('timing')
        ]))
        user_row = [user_row_dict.get(h) for h in user_headers]
        cell = user_management_sheet.find(user_id, in_column=1)
        if cell:
            range_to_update = f'A{cell.row}:{chr(ord("A") + len(user_row) - 1)}{cell.row}'
            user_management_sheet.update(range_to_update, [user_row])
        else:
            user_management_sheet.append_row(user_row)
    except Exception as e:
        print(f"ユーザー管理シートへの書き込みエラー: {e}")

    now = datetime.now()
    two_hours_later = now + timedelta(hours=2)
    send_time_today = now.replace(hour=21, minute=30, second=0, microsecond=0)
    target_send_time = send_time_today
    if target_send_time < two_hours_later:
        target_send_time += timedelta(days=1)
    wait_seconds = (target_send_time - now).total_seconds()

    thread = threading.Thread(target=lambda: (time.sleep(wait_seconds), send_delayed_offer(user_id, user_wishes)))
    thread.start()
    
    return jsonify({"status": "success", "message": "Offer task scheduled"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)