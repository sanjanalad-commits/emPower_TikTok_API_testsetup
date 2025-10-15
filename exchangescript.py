cat > exchange_token.py << 'EOF'
import requests
import urllib.parse

url = input("Paste the localhost URL here: ").strip()
parsed = urllib.parse.urlparse(url)
params = urllib.parse.parse_qs(parsed.query)
auth_code = params.get('auth_code', [None])[0]

if not auth_code:
    print("ERROR: No auth_code found in URL")
    exit(1)

print(f"✅ Found auth_code: {auth_code[:20]}...")

APP_ID = "7561256923966750737"  # ← CORRECTED App ID
APP_SECRET = input("\nPaste your FULL App Secret: ").strip()

print("\n🔄 Exchanging auth code for access token...")

response = requests.post(
    'https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/',
    json={"app_id": APP_ID, "secret": APP_SECRET, "auth_code": auth_code}
)

result = response.json()

if result.get('code') == 0:
    print("\n" + "=" * 60)
    print("🎉 SUCCESS! Your TikTok API Credentials:")
    print("=" * 60)
    print(f"\nACCESS_TOKEN: {result['data']['access_token']}")
    print(f"\nADVERTISER_ID: {result['data']['advertiser_id']}")
    print("\n" + "=" * 60)
    print("📋 Copy these into your tiktok_extractor.py!")
else:
    print(f"\n❌ ERROR: {result.get('message')}")
    print(f"Response: {result}")
EOF
