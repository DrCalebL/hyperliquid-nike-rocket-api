"""
Nike Rocket Hyperliquid - Email Service
=========================================

Sends welcome emails and API key resend emails.
Uses SendGrid or falls back to logging.

Author: Nike Rocket Team
"""

import os
import logging

logger = logging.getLogger(__name__)

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
FROM_EMAIL = os.getenv("FROM_EMAIL", "noreply@nikerocket.com")


def send_welcome_email(email: str, api_key: str) -> bool:
    """Send welcome email with API key to new user"""
    try:
        if not SENDGRID_API_KEY:
            logger.info(f"📧 [DEV] Welcome email for {email}: API key = {api_key[:20]}...")
            return True
        
        import sendgrid
        from sendgrid.helpers.mail import Mail
        
        sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)
        
        message = Mail(
            from_email=FROM_EMAIL,
            to_emails=email,
            subject="🚀 Welcome to Nike Rocket (Hyperliquid)",
            html_content=f"""
            <h2>Welcome to Nike Rocket!</h2>
            <p>Your trading agent API key:</p>
            <code style="background:#f0f0f0;padding:10px;display:block;font-size:16px;">{api_key}</code>
            <p>Keep this key safe - you'll need it to set up your agent.</p>
            <p><strong>Next steps:</strong></p>
            <ol>
                <li>Go to the Setup page</li>
                <li>Enter your API key</li>
                <li>Enter your Hyperliquid wallet credentials</li>
                <li>Your agent will start automatically!</li>
            </ol>
            <p>Happy trading! 🐷🚀</p>
            """
        )
        
        response = sg.send(message)
        logger.info(f"✅ Welcome email sent to {email} (status: {response.status_code})")
        return response.status_code in [200, 201, 202]
        
    except Exception as e:
        logger.error(f"❌ Failed to send welcome email to {email}: {e}")
        return False


def send_api_key_resend_email(email: str, api_key: str) -> bool:
    """Resend API key to existing user"""
    try:
        if not SENDGRID_API_KEY:
            logger.info(f"📧 [DEV] API key resend for {email}: API key = {api_key[:20]}...")
            return True
        
        import sendgrid
        from sendgrid.helpers.mail import Mail
        
        sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)
        
        message = Mail(
            from_email=FROM_EMAIL,
            to_emails=email,
            subject="🔑 Nike Rocket - Your API Key",
            html_content=f"""
            <h2>Your Nike Rocket API Key</h2>
            <p>Here's your API key as requested:</p>
            <code style="background:#f0f0f0;padding:10px;display:block;font-size:16px;">{api_key}</code>
            <p>If you didn't request this, you can safely ignore this email.</p>
            """
        )
        
        response = sg.send(message)
        logger.info(f"✅ API key resend email sent to {email}")
        return response.status_code in [200, 201, 202]
        
    except Exception as e:
        logger.error(f"❌ Failed to send API key email to {email}: {e}")
        return False
