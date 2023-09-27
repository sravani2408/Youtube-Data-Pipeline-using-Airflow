{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 33,
   "id": "b26974a4-58c0-48d0-99be-6dd6b0a69d6e",
   "metadata": {},
   "outputs": [],
   "source": [
    "import tweepy\n",
    "import pandas as pd\n",
    "import json\n",
    "from datetime import datetime\n",
    "import s3fs"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 42,
   "id": "2b2e58d1-4b43-43c5-ab2b-d47d2802cd51",
   "metadata": {},
   "outputs": [],
   "source": [
    "import os\n",
    "import googleapiclient.discovery\n",
    "import pandas as pd\n",
    "\n",
    "def get_youtube_comments(video_id):\n",
    "    api_service_name = \"youtube\"\n",
    "    api_version = \"v3\"\n",
    "    DEVELOPER_KEY = \"AIzaSyDZHFYU6hJpuzqNe0lt7-KB4ynXnkNKXQg\"\n",
    "\n",
    "    youtube = googleapiclient.discovery.build(\n",
    "        api_service_name, api_version, developerKey=DEVELOPER_KEY)\n",
    "\n",
    "    def process_comments(response_items):\n",
    "        comments = []\n",
    "        for item in response_items:\n",
    "            comment = item['snippet']['topLevelComment']['snippet']\n",
    "            author = comment['authorDisplayName']\n",
    "            comment_text = comment['textOriginal']\n",
    "            publish_time = comment['publishedAt']\n",
    "            comment_info = {'Author': author, 'Comment': comment_text, 'Published At': publish_time}\n",
    "            comments.append(comment_info)\n",
    "        return comments\n",
    "\n",
    "    request = youtube.commentThreads().list(\n",
    "        part=\"snippet,replies\",\n",
    "        videoId=video_id\n",
    "    )\n",
    "    response = request.execute()\n",
    "\n",
    "    comments_list = process_comments(response['items'])\n",
    "\n",
    "    while response.get('nextPageToken', None):\n",
    "        request = youtube.commentThreads().list(\n",
    "            part='snippet',\n",
    "            videoId=video_id,\n",
    "            pageToken=response['nextPageToken']\n",
    "        )\n",
    "        response = request.execute()\n",
    "        comments_list.extend(process_comments(response['items']))\n",
    "\n",
    "    df = pd.DataFrame(comments_list)\n",
    "    return df\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    video_id = \"q8q3OFFfY6c\"  # Can replace with other video ID\n",
    "    df = get_youtube_comments(video_id)\n",
    "    df.to_csv(\"s3://sravani-airflow-youtube-bucket/youtube_data.csv\")\n"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
