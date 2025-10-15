cat > get_tiktok_token.py << 'EOF'
import urllib.parse

APP_ID = "7561256923966750737"  # ← CORRECTED
REDIRECT_URI = "http://localhost:8080/callback"

auth_url = f"https://ads.tiktok.com/marketing_api/auth?app_id={APP_ID}&redirect_uri={urllib.parse.quote(REDIRECT_URI)}&state=getting_token"

print("=" * 60)
print("Copy this URL and open it in your browser:")
print("=" * 60)
print(auth_url)
print("\nAfter authorizing, copy the localhost URL from address bar")
print("=" * 60)
EOF

python3 get_tiktok_token.py
