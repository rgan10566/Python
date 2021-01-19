import os
from slack_bolt import App

# Initializes your app with your bot token and signing secret
app = App(
    token=os.environ.get("SLACK_BOT_TOKEN"),
    signing_secret=os.environ.get("SLACK_SIGNING_SECRET")
)

# Listens to incoming messages that contain "hello"
@app.message("hello")
def message_hello(message, client, logger):
    try:
        result = client.chat_postMessage(channel=message["channel"], text=f"Hey there <@{message['user']}>!")
        logger.info(result)
    except SlackApiError as e:
        logger.error(f"Error posting message: {e}")

# Start your app
if __name__ == "__main__":
    app.start(port=int(os.environ.get("PORT", 3000)))
