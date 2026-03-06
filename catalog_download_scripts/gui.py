import streamlit as st
import pandas as pd
from streamlit_server_state import (
    server_state,
    server_state_lock,
    force_rerun_bound_sessions,
)
import sqlite3

print("GUI Refresh")
st.set_page_config(layout="wide")

with server_state_lock.db:
    if "db" not in server_state:
        server_state.db = sqlite3.connect("history.db", check_same_thread=False)

with server_state_lock.sel_rapper:
    if "sel_rapper" not in server_state:
        server_state.sel_rapper = []

with server_state_lock.sel_platforms:
    if "sel_platforms" not in server_state:
        server_state.sel_platforms = []

with server_state_lock.sel_beat_type:
    if "sel_beat_type" not in server_state:
        server_state.sel_beat_type = 0

with server_state_lock.sel_match_type:
    if "sel_match_type" not in server_state:
        server_state.sel_match_type = 0

clicks_init = False
with server_state_lock.clicks:
    if "clicks" not in server_state:
        server_state.clicks = (0, 0)
        clicks_init = False

likes_init = False
with server_state_lock.likes:
    if "likes" not in server_state:
        server_state.likes = (0, 0)
        likes_init = False

comments_Init = False
with server_state_lock.comments:
    if "comments" not in server_state:
        server_state.comments = (-1, 0)
        comments_Init = False

query_params = []
query = """
SELECT
    b.video_title,
    b.video_url,
    b.video_release,
    r1.name AS rapper_1,
    r2.name AS rapper_2,
    b.video_clicks,
    b.video_likes,
    b.video_comments
FROM battles b
JOIN battle_participants bp1 ON b.id = bp1.battle_id
JOIN battle_participants bp2 ON b.id = bp2.battle_id
JOIN battlerappers r1 ON bp1.rapper_id = r1.id
JOIN battlerappers r2 ON bp2.rapper_id = r2.id
WHERE r1.id < r2.id  -- vermeidet doppelte Paarungen wie A vs B und B vs A
"""
rapper_query = """
SELECT 
    r.id,
    r.name,
    COALESCE(SUM(b.video_clicks), 0) AS total_clicks
FROM battlerappers r
LEFT JOIN battle_participants bp ON r.id = bp.rapper_id
LEFT JOIN battles b ON bp.battle_id = b.id
GROUP BY r.id
ORDER BY total_clicks DESC
"""
rapper_df = pd.read_sql_query(rapper_query, server_state.db)
rapper_list = rapper_df["name"].tolist()

# region Sidebar
# st.sidebar.header("🔎 Filteroptionen")

# region Rapper

selected_rapper = st.sidebar.multiselect(
    "🔍 Suche nach Rapper*innen",
    rapper_list,
    default=server_state.sel_rapper,
    label_visibility="visible",
)
with server_state_lock.sel_rapper:
    server_state.sel_rapper = selected_rapper.copy()

if len(selected_rapper) > 0:
    query += " AND ("
    selected_rapper_match = []
    for idx, rapper in enumerate(selected_rapper):
        selected_rapper_match.append("%" + rapper + "%")
        selected_rapper_match.append("%" + rapper + "%")
        query += " (r1.name LIKE ? OR r2.name LIKE ?) "
        if idx != len(selected_rapper) - 1:
            query += " OR "
    query += " ) "
    query_params.extend(selected_rapper_match)
# endregion
# region Platforms
platforms_query = """SELECT * from platforms"""
platforms_df = pd.read_sql_query(platforms_query, server_state.db)
platforms_list = platforms_df["name"].tolist()
selected_platforms = st.sidebar.multiselect(
    "📺 Plattform wählen", platforms_list, default=server_state.sel_platforms
)
with server_state_lock.sel_platforms:
    server_state.sel_platforms = selected_platforms.copy()

platform_map = dict(zip(platforms_df["name"], platforms_df["id"]))
selected_platform_ids = [platform_map[name] for name in selected_platforms]
if len(selected_platform_ids) > 0:
    query += f" AND b.platform_id IN ({','.join(['?'] * len(selected_platforms))})\n"
    query_params.extend(selected_platform_ids)
# endregion
# region On-Beat
beat_option_list = ["Acapella + Onbeat", "Onbeat", "Acapella"]
onbeat_option = st.sidebar.selectbox(
    "🎵 Beat-Typ", beat_option_list, index=server_state.sel_beat_type
)
with server_state_lock.sel_beat_type:
    server_state.sel_beat_type = beat_option_list.index(onbeat_option)
if onbeat_option == "Onbeat":
    query += " AND b.on_beat = 1\n"
elif onbeat_option == "Acapella":
    query += " AND b.on_beat = 0\n"
# endregion
# region Match-Style (Written/Acapella)
written_option_list = ["Written + Freestyle", "Written", "Freestyle"]
written_option = st.sidebar.selectbox(
    "Match-Type", written_option_list, index=server_state.sel_match_type
)
with server_state_lock.sel_match_type:
    server_state.sel_match_type = written_option_list.index(written_option)
if written_option == "Written":
    query += " AND b.is_freestyle = 0\n"
elif written_option == "Freestyle":
    query += " AND b.is_freestyle = 1\n"
# endregion
# region Clicks, Likes, Comments
min_clicks, max_clicks = server_state.db.execute(
    """SELECT MIN(video_clicks) AS min_clicks, MAX(video_clicks) AS max_clicks FROM battles;"""
).fetchone()
if not clicks_init:
    clicks_init = True
    with server_state_lock.clicks:
        server_state.clicks = (min_clicks, max_clicks)

min_clicks_selected, max_clicks_selected = st.sidebar.slider(
    "Klicks", min_clicks, max_clicks, (min_clicks, max_clicks)
)
query += (
    "AND b.video_clicks BETWEEN "
    + str(min_clicks_selected)
    + " AND "
    + str(max_clicks_selected)
    + "\n"
)
with server_state_lock.clicks:
    server_state.clicks = (min_clicks_selected, max_clicks_selected)

min_likes, max_likes = server_state.db.execute(
    """SELECT MIN(video_likes) AS min_likes, MAX(video_likes) AS max_likes FROM battles;"""
).fetchone()

if not likes_init:
    likes_init = True
    with server_state_lock.likes:
        server_state.likes = (min_likes, max_likes)
min_likes_selected, max_likes_selected = st.sidebar.slider(
    "Likes", min_likes, max_likes, (min_likes, max_likes)
)
query += (
    "AND b.video_likes BETWEEN "
    + str(min_likes_selected)
    + " AND "
    + str(max_likes_selected)
    + "\n"
)
with server_state_lock.likes:
    server_state.likes = (min_likes_selected, max_likes_selected)

min_comments, max_comments = server_state.db.execute(
    """SELECT MIN(video_comments) AS min_comments, MAX(video_comments) AS max_comments FROM battles;"""
).fetchone()
if not comments_Init:
    comments_Init = True
    with server_state_lock.comments:
        server_state.comments = (min_comments, max_comments)
min_comments_selected, max_comments_selected = st.sidebar.slider(
    "Kommentare", min_comments, max_comments, (min_comments, max_comments)
)
query += (
    "AND b.video_comments BETWEEN "
    + str(min_comments_selected)
    + " AND "
    + str(max_comments_selected)
    + "\n"
)
with server_state_lock.comments:
    server_state.comments = (min_comments_selected, max_comments_selected)
# endregion
# endregion

query += "ORDER BY b.video_release DESC;"

# print(query)

df = pd.read_sql_query(query, server_state.db, params=query_params)
for rapper in selected_rapper:
    mask = ~df["rapper_1"].str.contains(rapper, case=False, na=False) & df[
        "rapper_2"
    ].str.contains(rapper, case=False, na=False)
    df.loc[mask, ["rapper_1", "rapper_2"]] = df.loc[
        mask, ["rapper_2", "rapper_1"]
    ].values

st.dataframe(
    df,
    column_config={
        "video_title": "Video Name",
        "video_release": st.column_config.DatetimeColumn(
            label="Erscheinungdatum (Youtube)", format="DD.MM.YYYY"
        ),
        "rapper_1": "Rapper*in 1",
        "rapper_2": "Rapper*in 2",
        "video_clicks": "Klicks",
        "video_likes": "Likes",
        "video_comments": "YoutubeKommentare",
        "video_url": st.column_config.LinkColumn("URL"),
    },
    use_container_width=True,
    height=700,
    # hide_index=True,
)
