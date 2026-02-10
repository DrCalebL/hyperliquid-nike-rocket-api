"""
Email Service - CORRECTED VERSION
==================================
Numbered steps in correct order:
1. Setup Agent
2. View Dashboard  
3. Access Anytime

Updated: November 22, 2025 - FINAL CORRECTED
"""

import os
import requests
from typing import Optional

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
RESEND_API_URL = "https://api.resend.com/emails"
FROM_EMAIL = os.getenv("FROM_EMAIL", "$NIKEPIG's Massive Rocket <noreply@rocket.nikepig.com>")
BASE_URL = os.getenv("BASE_URL", "https://rocket-hla.nikepig.com")


def send_welcome_email(to_email: str, api_key: str) -> bool:
    """
    Send welcome email with API key
    
    Order:
    1. Setup Agent (FIRST!)
    2. View Dashboard (2nd last)
    3. Access Anytime (last)
    """
    if not RESEND_API_KEY:
        print("⚠️ RESEND_API_KEY not set - email not sent")
        return False
    
    setup_link = f"{BASE_URL}/setup?key={api_key}"
    dashboard_link = f"{BASE_URL}/dashboard?key={api_key}"
    login_link = f"{BASE_URL}/login"
    
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; background: #f5f5f5;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background: #f5f5f5; padding: 40px 20px;">
        <tr>
            <td align="center">
                <table width="600" cellpadding="0" cellspacing="0" style="max-width: 600px; background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.1);">
                    
                    <!-- Header -->
                    <tr>
                        <td style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 40px 30px; text-align: center;">
                            <h1 style="margin: 0; color: white; font-size: 32px; font-weight: bold;">
                                🚀 $NIKEPIG's Massive Rocket
                            </h1>
                            <p style="margin: 8px 0 0 0; color: rgba(255,255,255,0.9); font-size: 16px;">
                                Your API Key
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Body -->
                    <tr>
                        <td style="padding: 40px 30px;">
                            
                            <!-- API Key -->
                            <h2 style="margin: 0 0 20px 0; color: #667eea; font-size: 20px;">
                                Your API Key
                            </h2>
                            <p style="margin: 0 0 15px 0; color: #374151; font-size: 14px;">
                                As requested, here's your $NIKEPIG's Massive Rocket API key:
                            </p>
                            
                            <div style="background: #f9fafb; border: 2px dashed #d1d5db; border-radius: 8px; padding: 20px; margin-bottom: 25px;">
                                <code style="font-family: 'Courier New', monospace; font-size: 14px; color: #667eea; word-break: break-all; display: block; text-align: center;">
                                    {api_key}
                                </code>
                            </div>
                            
                            <div style="background: #fef3c7; border-left: 4px solid #f59e0b; padding: 15px; border-radius: 6px; margin-bottom: 35px;">
                                <p style="margin: 0; color: #92400e; font-size: 13px;">
                                    🔒 <strong>Security Reminder:</strong> Never share your API key with anyone.
                                </p>
                            </div>
                            
                            <!-- NUMBERED STEPS - CORRECT ORDER! -->
                            
                            <!-- Step 1: Setup Agent (FIRST!) -->
                            <div style="padding: 18px 0; border-bottom: 1px solid #e5e7eb;">
                                <p style="margin: 0 0 8px 0; color: #374151; font-size: 15px; font-weight: 600;">
                                    1. Setup Agent
                                </p>
                                <p style="margin: 0 0 10px 0; color: #6b7280; font-size: 14px;">
                                    Configure your trading agent (takes 2 minutes)
                                </p>
                                <p style="margin: 0;">
                                    <a href="{setup_link}" style="color: #667eea; text-decoration: none; font-weight: 600; font-size: 14px;">
                                        → Setup Agent
                                    </a>
                                </p>
                            </div>
                            
                            <!-- Step 2: View Dashboard (2nd last!) -->
                            <div style="padding: 18px 0; border-bottom: 1px solid #e5e7eb;">
                                <p style="margin: 0 0 8px 0; color: #374151; font-size: 15px; font-weight: 600;">
                                    2. View Dashboard
                                </p>
                                <p style="margin: 0 0 10px 0; color: #6b7280; font-size: 14px;">
                                    Track your performance in real-time
                                </p>
                                <p style="margin: 0;">
                                    <a href="{dashboard_link}" style="color: #667eea; text-decoration: none; font-weight: 600; font-size: 14px;">
                                        → View Dashboard
                                    </a>
                                </p>
                            </div>
                            
                            <!-- Step 3: Access Anytime (last!) -->
                            <div style="padding: 18px 0;">
                                <p style="margin: 0 0 8px 0; color: #374151; font-size: 15px; font-weight: 600;">
                                    3. Access Anytime:
                                </p>
                                <p style="margin: 0;">
                                    <a href="{login_link}" style="color: #667eea; text-decoration: none; font-weight: 600; font-size: 14px;">
                                        → {login_link}
                                    </a>
                                </p>
                            </div>
                            
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="padding: 30px; background: #f9fafb; text-align: center; border-top: 1px solid #e5e7eb;">
                            <p style="margin: 0; color: #6b7280; font-size: 13px;">
                                Questions? Contact us anytime.
                            </p>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
    """
    
    text_content = f"""
🚀 Your $NIKEPIG's Massive Rocket API Key

Your API Key:
{api_key}

🔒 Security Reminder: Never share your API key.

1. Setup Agent
   → {setup_link}

2. View Dashboard
   → {dashboard_link}

3. Access Anytime:
   → {login_link}
    """
    
    try:
        response = requests.post(
            RESEND_API_URL,
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "from": FROM_EMAIL,
                "to": [to_email],
                "subject": "🚀 Your $NIKEPIG's Massive Rocket API Key",
                "html": html_content,
                "text": text_content
            }
        )
        
        if response.status_code == 200:
            print(f"✅ Welcome email sent to {to_email}")
            return True
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def send_api_key_resend_email(to_email: str, api_key: str) -> bool:
    """Resend API key - SAME FORMAT AS WELCOME!"""
    if not RESEND_API_KEY:
        return False
    
    setup_link = f"{BASE_URL}/setup?key={api_key}"
    dashboard_link = f"{BASE_URL}/dashboard?key={api_key}"
    login_link = f"{BASE_URL}/login"
    
    # SAME HTML AS WELCOME EMAIL!
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; background: #f5f5f5;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background: #f5f5f5; padding: 40px 20px;">
        <tr>
            <td align="center">
                <table width="600" cellpadding="0" cellspacing="0" style="max-width: 600px; background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.1);">
                    
                    <tr>
                        <td style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 40px 30px; text-align: center;">
                            <h1 style="margin: 0; color: white; font-size: 32px;">🚀 $NIKEPIG's Massive Rocket</h1>
                            <p style="margin: 8px 0 0 0; color: rgba(255,255,255,0.9);">Your API Key</p>
                        </td>
                    </tr>
                    
                    <tr>
                        <td style="padding: 40px 30px;">
                            <h2 style="margin: 0 0 20px 0; color: #667eea;">Your API Key</h2>
                            <p style="margin: 0 0 15px 0; color: #374151; font-size: 14px;">
                                As requested, here's your API key:
                            </p>
                            
                            <div style="background: #f9fafb; border: 2px dashed #d1d5db; border-radius: 8px; padding: 20px; margin-bottom: 25px;">
                                <code style="font-family: 'Courier New', monospace; font-size: 14px; color: #667eea; word-break: break-all; display: block; text-align: center;">
                                    {api_key}
                                </code>
                            </div>
                            
                            <div style="background: #fef3c7; border-left: 4px solid #f59e0b; padding: 15px; border-radius: 6px; margin-bottom: 35px;">
                                <p style="margin: 0; color: #92400e; font-size: 13px;">
                                    🔒 <strong>Security Reminder:</strong> Never share your API key.
                                </p>
                            </div>
                            
                            <!-- SAME NUMBERED STEPS! -->
                            <div style="padding: 18px 0; border-bottom: 1px solid #e5e7eb;">
                                <p style="margin: 0 0 8px 0; color: #374151; font-size: 15px; font-weight: 600;">
                                    1. Setup Agent
                                </p>
                                <p style="margin: 0;">
                                    <a href="{setup_link}" style="color: #667eea; text-decoration: none; font-weight: 600;">
                                        → Setup Agent
                                    </a>
                                </p>
                            </div>
                            
                            <div style="padding: 18px 0; border-bottom: 1px solid #e5e7eb;">
                                <p style="margin: 0 0 8px 0; color: #374151; font-size: 15px; font-weight: 600;">
                                    2. View Dashboard
                                </p>
                                <p style="margin: 0;">
                                    <a href="{dashboard_link}" style="color: #667eea; text-decoration: none; font-weight: 600;">
                                        → View Dashboard
                                    </a>
                                </p>
                            </div>
                            
                            <div style="padding: 18px 0;">
                                <p style="margin: 0 0 8px 0; color: #374151; font-size: 15px; font-weight: 600;">
                                    3. Access Anytime:
                                </p>
                                <p style="margin: 0;">
                                    <a href="{login_link}" style="color: #667eea; text-decoration: none; font-weight: 600;">
                                        → {login_link}
                                    </a>
                                </p>
                            </div>
                        </td>
                    </tr>
                    
                    <tr>
                        <td style="padding: 20px 30px; background: #f9fafb; text-align: center; border-top: 1px solid #e5e7eb;">
                            <p style="margin: 0; color: #6b7280; font-size: 12px;">
                                If you didn't request this, contact support.
                            </p>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
    """
    
    try:
        response = requests.post(
            RESEND_API_URL,
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "from": FROM_EMAIL,
                "to": [to_email],
                "subject": "Your $NIKEPIG's Massive Rocket API Key",
                "html": html_content
            }
        )
        return response.status_code == 200
    except:
        return False


# Deprecated functions
def send_verification_email(to_email: str, verification_token: str) -> bool:
    return False

def send_api_key_email(to_email: str, api_key: str) -> bool:
    return send_welcome_email(to_email, api_key)

def send_password_reset_email(to_email: str, reset_token: str) -> bool:
    return False
