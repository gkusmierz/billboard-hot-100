# Spotify API Setup Guide

This guide explains how to set up Spotify API credentials to enable automatic fetching of Spotify track URLs for the Billboard Hot 100 #1 songs.

## Prerequisites

1. A Spotify account (free or premium)
2. Python 3.6+ with `requests` library

## Step 1: Create a Spotify App

1. Go to the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Log in with your Spotify account
3. Click "Create an App"
4. Fill in the app details:
   - **App name**: Billboard Hot 100 Tracker (or any name you prefer)
   - **App description**: Fetches Spotify URLs for Billboard #1 songs
   - **Website**: Leave blank or add your website
   - **Redirect URI**: Not needed for this application
5. Accept the terms and create the app

## Step 2: Get Your Credentials

1. Once your app is created, you'll see the app dashboard
2. Click "Settings" in the top right
3. You'll see your **Client ID** and **Client Secret**
4. Copy these values - you'll need them for configuration

## Step 3: Configure the Application

You have two options to provide your Spotify credentials:

### Option A: Environment Variables (Recommended)

Set the following environment variables:

```bash
export SPOTIFY_CLIENT_ID="your_client_id_here"
export SPOTIFY_CLIENT_SECRET="your_client_secret_here"
```

On Windows:
```cmd
set SPOTIFY_CLIENT_ID=your_client_id_here
set SPOTIFY_CLIENT_SECRET=your_client_secret_here
```

### Option B: Configuration File

1. Copy the example config file:
   ```bash
   cp spotify_config.json.example spotify_config.json
   ```

2. Edit `spotify_config.json` and replace the placeholder values:
   ```json
   {
     "client_id": "your_actual_client_id_here",
     "client_secret": "your_actual_client_secret_here"
   }
   ```

**Important**: Never commit `spotify_config.json` to version control as it contains sensitive credentials.

## Step 4: Install Dependencies

Make sure you have the required Python packages:

```bash
pip install requests
```

## Step 5: Run the Script

Now you can run the hot-ones.py script with Spotify integration:

```bash
python3 hot-ones.py
```

The script will:
1. Process all Billboard chart data
2. Extract #1 songs for each week
3. Search for each unique song on Spotify
4. Add Spotify track URLs to the `distinct-hots.json` file

## Rate Limiting

The script includes built-in rate limiting (0.1 seconds between requests) to respect Spotify's API limits. For 1,179 unique songs, the Spotify search process will take approximately 2-3 minutes.

## Troubleshooting

### "No Spotify credentials found" Error
- Make sure you've set the environment variables correctly
- Or ensure `spotify_config.json` exists and contains valid credentials

### "Error getting access token" Error
- Verify your Client ID and Client Secret are correct
- Check that your Spotify app is properly configured
- Ensure you have an internet connection

### Songs Not Found on Spotify
- Some older songs or songs with special characters might not be found
- The script will continue processing and mark unfound songs with `"trackId": null`
- This is normal and expected for some tracks

## API Limits

Spotify's Web API has the following limits:
- **Rate limiting**: The script respects these limits with built-in delays
- **Daily requests**: Should be sufficient for this use case
- **Search results**: Limited to most relevant matches

For more information, see the [Spotify Web API documentation](https://developer.spotify.com/documentation/web-api/).