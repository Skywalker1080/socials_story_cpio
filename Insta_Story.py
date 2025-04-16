import os
import json
from instagrapi import Client

# Get credentials from GitHub Actions secrets
# These will be set in the GitHub repository settings and referenced in your workflow file
username = os.getenv("IG_USERNAME")
password = os.getenv("IG_PASSWORD")
settings_json_str = os.getenv("IG_SETTINGS_JSON")

print("🔄 Initializing Instagram client...")
cl = Client()

# Try to load settings from the environment variable if available
if settings_json_str:
    try:
        settings = json.loads(settings_json_str)
        cl.set_settings(settings)
        print("✅ Loaded existing settings")
    except json.JSONDecodeError:
        print("⚠️ Invalid settings JSON format")
    except Exception as e:
        print(f"⚠️ Error loading settings: {str(e)}")

# Login process
try:
    # Try login with the settings if they were loaded
    cl.login(username, password)
    print("✅ Logged in successfully")
except Exception as e:
    print(f"⚠️ Login failed: {str(e)}")
    print("🔄 Trying fresh login...")
    cl = Client()
    cl.login(username, password)
    print("✅ Fresh login successful")

# Save the current settings (this can be useful for debugging or future runs)
settings_json = json.dumps(cl.get_settings())
print("✅ Session ready")

# In GitHub Actions, you'll need to specify where images are stored
# This could be in the repo or downloaded during the workflow
image_paths = [
    "./Airdrop.jpg"  # Adjust path based on your GitHub Actions workflow structure
]

# Loop through the image paths and upload each as a story
for image_path in image_paths:
    try:
        media = cl.photo_upload_to_story(image_path)
        print(f"✅ Story uploaded successfully for {image_path}: {media.id}")
    except Exception as e:
        print(f"❌ Error uploading story for {image_path}: {e}")
        # Exit with error code to make the GitHub Action fail properly
        if os.getenv("GITHUB_ACTIONS") == "true":
            exit(1)
