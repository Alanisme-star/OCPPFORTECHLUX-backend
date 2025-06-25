# utils/line_notify.py

import os
import requests

def send_line_notify(message: str):
    """發送 LINE Notify 訊息"""
    token = os.getenv("LINE_NOTIFY_TOKEN")
    if not token:
        print("⚠️ 未設定 LINE_NOTIFY_TOKEN，略過通知")
        return

    url = "https://notify-api.line.me/api/notify"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    data = {"message": message}

    try:
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            print("✅ LINE Notify 發送成功")
        else:
            print(f"❌ 發送失敗，狀態碼: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ 發送 LINE Notify 發生錯誤：{e}")
