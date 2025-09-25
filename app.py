from flask import Flask, render_template, request, redirect, url_for, session, send_file, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect, func
import os
import pandas as pd
from datetime import datetime, timezone, timedelta
import uuid
import io
import threading
from googleapiclient.discovery import build
import json

basedir = os.path.abspath(os.path.dirname(__file__))
app = Flask(__name__)

# --- 앱 설정 ---
app.secret_key = 'a-new-secret-key-for-pagination'
db_url = os.environ.get('DATABASE_URL')
if db_url and db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)
app.config['SQLALCHEMY_DATABASE_URI'] = db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- 외부 설정 및 전역 변수 ---
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')
CRAWL_STATUS = {'is_running': False, 'progress': '대기 중'}
KST = timezone(timedelta(hours=9))


# --- 데이터베이스 모델 정의 ---
class LoginUser(db.Model):
    __tablename__ = 'login_user'
    id = db.Column(db.String(80), primary_key=True)
    name = db.Column(db.String(80), nullable=False)
    password = db.Column(db.String(80), nullable=False)

class Shorts(db.Model):
    __tablename__ = 'shorts'
    seq = db.Column(db.Integer, primary_key=True, autoincrement=True)
    url = db.Column(db.String(200), nullable=False, unique=True)
    channel_name = db.Column(db.String(100))
    channel_profile_url = db.Column(db.String(200))
    description = db.Column(db.String(200))
    use_yn = db.Column(db.String(1), default='Y', nullable=False)

class YoutubeComment(db.Model):
    __tablename__ = 'youtube_comment'
    seq = db.Column(db.Integer, primary_key=True, autoincrement=True)
    shorts_url = db.Column(db.String(200), nullable=False)
    comment_id = db.Column(db.String(100), nullable=False)
    parent_id = db.Column(db.String(100), nullable=True)
    author_name = db.Column(db.String(100))
    comment_text = db.Column(db.Text)
    published_at = db.Column(db.String(50))
    like_count = db.Column(db.Integer)
    author_profile_image_url = db.Column(db.String(200))

class EventLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    login_id = db.Column(db.String(80), nullable=False)
    shorts_url = db.Column(db.String(200), nullable=False)
    event_timestamp = db.Column(db.String(50), nullable=False)
    event_type = db.Column(db.String(50), nullable=False)
    session_id = db.Column(db.String(100), nullable=False)

class ShortsActivity(db.Model):
    __tablename__ = 'shorts_activity'
    id = db.Column(db.Integer, primary_key=True)
    login_id = db.Column(db.String(80), nullable=False)
    shorts_url = db.Column(db.String(200), nullable=False)
    like = db.Column(db.Integer, default=0)
    dislike = db.Column(db.Integer, default=0)
    share = db.Column(db.Integer, default=0)
    interest = db.Column(db.Integer, default=0)
    recommend = db.Column(db.Integer, default=0)
    report = db.Column(db.Integer, default=0)
    subscribe = db.Column(db.Integer, default=0)
    __table_args__ = (db.UniqueConstraint('login_id', 'shorts_url', name='_login_shorts_uc'),)

class UserLastState(db.Model):
    __tablename__ = 'user_last_state'
    login_id = db.Column(db.String(80), primary_key=True)
    last_watched_url = db.Column(db.String(200))

MODELS = {
    'login_user': LoginUser, 'shorts': Shorts, 'event_log': EventLog,
    'shorts_activity': ShortsActivity, 'user_last_state': UserLastState,
    'youtube_comment': YoutubeComment
}


# --- 사용자 페이지 라우팅 ---
@app.route('/')
def login_page():
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def handle_login():
    user_id = request.form.get('user_id')
    password = request.form.get('password', '')
    if user_id == 'super_admin' and password == '0604':
        session['user_role'] = 'super_admin'
        return redirect(url_for('admin_page'))
    user = LoginUser.query.filter_by(id=user_id, password=password).first()
    if user:
        if user.id == 'admin':
            session['user_role'] = 'admin'
            return redirect(url_for('admin_page'))
        else:
            session['user_role'] = 'user'
            session['user_id'] = user.id
            session['session_id'] = str(uuid.uuid4())
            log_and_update_state(login_id=user.id, shorts_url='N/A', event_type='로그인', session_id=session['session_id'])
            return redirect(url_for('shorts_page'))
    else:
        return render_template('login.html', error_message="ID 또는 패스워드가 일치하지 않습니다.")

@app.route('/shorts')
def shorts_page():
    if session.get('user_role') != 'user': return redirect(url_for('login_page'))
    user_id = session['user_id']
    comment_counts_query = db.session.query(YoutubeComment.shorts_url, func.count(YoutubeComment.comment_id)).group_by(YoutubeComment.shorts_url).all()
    comment_counts = dict(comment_counts_query)
    shorts_list = Shorts.query.filter_by(use_yn='Y').all()
    shorts_data = []
    for s in shorts_list:
        shorts_data.append({'seq': s.seq, 'url': s.url, 'channel_name': s.channel_name, 'channel_profile_url': s.channel_profile_url, 'description': s.description, 'video_id': s.url.split('?')[0].split('/shorts/')[-1], 'comment_count': comment_counts.get(s.url, 0)})
    last_state = UserLastState.query.filter_by(login_id=user_id).first()
    activities = ShortsActivity.query.filter_by(login_id=user_id).all()
    activity_map = {a.shorts_url: {'좋아요': a.like, '싫어요': a.dislike, '공유': a.share, '관심없음': a.interest, '채널추천안함': a.recommend, '신고': a.report, '구독': a.subscribe} for a in activities}
    return render_template('index.html', shorts=shorts_data, user_id=user_id, session_id=session['session_id'], last_watched_url=last_state.last_watched_url if last_state else None, activity_map=activity_map)

@app.route('/log_event', methods=['POST'])
def log_event_from_js():
    data = request.json
    log_and_update_state(login_id=data['login_id'], shorts_url=data['shorts_url'], event_type=data['event_type'], session_id=data['session_id'])
    return jsonify(success=True)

def log_and_update_state(login_id, shorts_url, event_type, session_id):
    timestamp = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    db.session.add(EventLog(login_id=login_id, shorts_url=shorts_url, event_timestamp=timestamp, event_type=event_type, session_id=session_id))
    if event_type not in ['로그인', '시청시작', '시청중지_종료', '댓글클릭', '댓글닫기클릭']:
        activity = ShortsActivity.query.filter_by(login_id=login_id, shorts_url=shorts_url).first()
        if not activity:
            activity = ShortsActivity(login_id=login_id, shorts_url=shorts_url)
            db.session.add(activity)
        is_cancel = '취소' in event_type
        state_value = 0 if is_cancel else 1
        if '좋아요' in event_type:
            activity.like = state_value
            if not is_cancel: activity.dislike = 0
        elif '싫어요' in event_type:
            activity.dislike = state_value
            if not is_cancel: activity.like = 0
        elif '공유' in event_type: activity.share = state_value
        elif '관심없음' in event_type: activity.interest = state_value
        elif '채널추천안함' in event_type: activity.recommend = state_value
        elif '신고' in event_type: activity.report = state_value
        elif '구독' in event_type: activity.subscribe = state_value
    if event_type == '시청시작':
        last_state = UserLastState.query.filter_by(login_id=login_id).first()
        if not last_state:
            last_state = UserLastState(login_id=login_id)
            db.session.add(last_state)
        last_state.last_watched_url = shorts_url
    db.session.commit()

@app.route('/get_comments')
def get_comments():
    if session.get('user_role') != 'user': return jsonify(error="Not authorized"), 403
    shorts_url = request.args.get('url')
    page = request.args.get('page', 1, type=int)
    per_page = 20

    pagination = YoutubeComment.query.filter_by(shorts_url=shorts_url, parent_id=None).order_by(YoutubeComment.published_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    top_level_comments = pagination.items
    
    top_level_comment_ids = [c.comment_id for c in top_level_comments]

    replies = YoutubeComment.query.filter(YoutubeComment.shorts_url==shorts_url, YoutubeComment.parent_id.in_(top_level_comment_ids)).all()
    
    replies_map = {}
    for reply in replies:
        if reply.parent_id not in replies_map:
            replies_map[reply.parent_id] = []
        replies_map[reply.parent_id].append(reply)

    comments_data = []
    for comment in top_level_comments:
        comment_dict = {
            "comment_id": comment.comment_id, "parent_id": comment.parent_id, "author_name": comment.author_name,
            "comment_text": comment.comment_text, "published_at": comment.published_at, "like_count": comment.like_count,
            "author_profile_image_url": comment.author_profile_image_url, "replies": []
        }
        if comment.comment_id in replies_map:
            for reply in replies_map[comment.comment_id]:
                comment_dict["replies"].append({
                    "comment_id": reply.comment_id, "parent_id": reply.parent_id, "author_name": reply.author_name,
                    "comment_text": reply.comment_text, "published_at": reply.published_at, "like_count": reply.like_count,
                    "author_profile_image_url": reply.author_profile_image_url,
                })
        comments_data.append(comment_dict)
    
    return jsonify({
        "comments": comments_data,
        "has_next_page": pagination.has_next
    })

@app.route('/add_comment', methods=['POST'])
def add_comment():
    if session.get('user_role') != 'user': return jsonify(error="Not authorized"), 403
    data = request.json
    shorts_url = data.get('shorts_url')
    comment_text = data.get('comment_text')
    parent_id = data.get('parent_id')
    user_id = session.get('user_id')
    if not all([shorts_url, comment_text, user_id]): return jsonify(error="Missing data"), 400
    new_comment = YoutubeComment(shorts_url=shorts_url, comment_id=f"user_comment_{uuid.uuid4()}", parent_id=parent_id, author_name=user_id, comment_text=comment_text, published_at=datetime.now(KST).isoformat(), like_count=0, author_profile_image_url=f"https://i.pravatar.cc/32?u={user_id}")
    db.session.add(new_comment)
    db.session.commit()
    return jsonify({
        "comment_id": new_comment.comment_id, "parent_id": new_comment.parent_id, "author_name": new_comment.author_name,
        "comment_text": new_comment.comment_text, "published_at": new_comment.published_at, "like_count": new_comment.like_count,
        "author_profile_image_url": new_comment.author_profile_image_url
    })


# --- 관리자 페이지 ---
def generate_measurement_results(search_login_id, search_shorts_url):
    logs_q = db.session.query(EventLog).statement
    activities_q = db.session.query(ShortsActivity).statement
    user_comments_q = db.session.query(YoutubeComment).filter(YoutubeComment.comment_id.like('user_comment_%')).statement
    logs = pd.read_sql(logs_q, db.engine)
    activities = pd.read_sql(activities_q, db.engine)
    user_comments = pd.read_sql(user_comments_q, db.engine)
    if not logs.empty: logs = logs[logs['shorts_url'] != 'N/A']
    all_pairs = []
    if not logs.empty: all_pairs.append(logs[['login_id', 'shorts_url']].drop_duplicates())
    if not activities.empty: all_pairs.append(activities[['login_id', 'shorts_url']].drop_duplicates())
    if not user_comments.empty:
        user_comments.rename(columns={'author_name': 'login_id'}, inplace=True)
        all_pairs.append(user_comments[['login_id', 'shorts_url']].drop_duplicates())
    if not all_pairs: return [], []
    base_df = pd.concat(all_pairs, ignore_index=True).drop_duplicates()
    if not logs.empty:
        logs['event_timestamp'] = pd.to_datetime(logs['event_timestamp'], errors='coerce')
        first_starts = logs[logs['event_type'] == '시청시작'].groupby(['login_id', 'shorts_url'])['event_timestamp'].min().reset_index()
        first_starts.rename(columns={'event_timestamp': 'start_time'}, inplace=True)
        last_likes = logs[logs['event_type'] == '좋아요'].groupby(['login_id', 'shorts_url'])['event_timestamp'].max().reset_index()
        last_likes.rename(columns={'event_timestamp': 'like_time'}, inplace=True)
        last_dislikes = logs[logs['event_type'] == '싫어요'].groupby(['login_id', 'shorts_url'])['event_timestamp'].max().reset_index()
        last_dislikes.rename(columns={'event_timestamp': 'dislike_time'}, inplace=True)
        time_to_action = pd.merge(first_starts, last_likes, on=['login_id', 'shorts_url'], how='left')
        time_to_action = pd.merge(time_to_action, last_dislikes, on=['login_id', 'shorts_url'], how='left')
        time_to_action['time_to_like'] = (time_to_action['like_time'] - time_to_action['start_time']).dt.total_seconds().round(1)
        time_to_action['time_to_dislike'] = (time_to_action['dislike_time'] - time_to_action['start_time']).dt.total_seconds().round(1)
    else:
        time_to_action = pd.DataFrame(columns=['login_id', 'shorts_url', 'time_to_like', 'time_to_dislike'])
    def calculate_sequential_duration(group_df, start_event, end_event):
        total_duration = 0
        start_time = None
        for _, row in group_df.iterrows():
            if row['event_type'] == start_event:
                if start_time is None: start_time = row['event_timestamp']
            elif row['event_type'] == end_event:
                if start_time is not None:
                    duration = (row['event_timestamp'] - start_time).total_seconds()
                    total_duration += duration
                    start_time = None 
        return total_duration
    watch_durations, comment_durations = pd.DataFrame(), pd.DataFrame()
    if not logs.empty:
        logs_sorted = logs.sort_values(by=['login_id', 'shorts_url', 'event_timestamp'])
        view_logs = logs_sorted[logs_sorted['event_type'].isin(['시청시작', '시청중지_종료'])]
        agg_watch = view_logs.groupby(['login_id', 'shorts_url']).apply(calculate_sequential_duration, '시청시작', '시청중지_종료')
        watch_durations = pd.DataFrame(agg_watch, columns=['duration']).reset_index()
        comment_logs = logs_sorted[logs_sorted['event_type'].isin(['댓글클릭', '댓글닫기클릭'])]
        agg_comment = comment_logs.groupby(['login_id', 'shorts_url']).apply(calculate_sequential_duration, '댓글클릭', '댓글닫기클릭')
        comment_durations = pd.DataFrame(agg_comment, columns=['duration']).reset_index()
    if not user_comments.empty:
        user_comments_agg = user_comments[['login_id', 'shorts_url']].assign(댓글작성=1).drop_duplicates()
    else:
        user_comments_agg = pd.DataFrame(columns=['login_id', 'shorts_url', '댓글작성'])
    result_df = base_df.copy()
    if not activities.empty: result_df = pd.merge(result_df, activities, on=['login_id', 'shorts_url'], how='left')
    if not watch_durations.empty: result_df = pd.merge(result_df, watch_durations, on=['login_id', 'shorts_url'], how='left')
    if not comment_durations.empty: result_df = pd.merge(result_df, comment_durations, on=['login_id', 'shorts_url'], how='left', suffixes=('_watch', '_comment'))
    if not user_comments_agg.empty: result_df = pd.merge(result_df, user_comments_agg, on=['login_id', 'shorts_url'], how='left')
    if not time_to_action.empty: result_df = pd.merge(result_df, time_to_action[['login_id', 'shorts_url', 'time_to_like', 'time_to_dislike']], on=['login_id', 'shorts_url'], how='left')
    result_df.rename(columns={'duration_watch': '시청시간(S)', 'duration': '시청시간(S)', 'duration_comment': '댓글시간(S)', 'like': '좋아요', 'dislike': '싫어요', 'share': '공유', 'interest': '관심없음', 'recommend': '채널추천안함', 'report': '신고'}, inplace=True)
    if 'duration' in result_df.columns and '시청시간(S)' not in result_df.columns: result_df.rename(columns={'duration': '시청시간(S)'}, inplace=True)
    final_columns = ['login_id', 'shorts_url', '시청시간(S)', '좋아요', '싫어요', '댓글시간(S)', '댓글작성', '공유', '관심없음', '채널추천안함', '신고']
    for col in final_columns:
        if col not in result_df.columns: result_df[col] = 0
    result_df.fillna(0, inplace=True)
    if '시청시간(S)' in result_df.columns: result_df['시청시간(S)'] = result_df['시청시간(S)'].round(1)
    if '댓글시간(S)' in result_df.columns: result_df['댓글시간(S)'] = result_df['댓글시간(S)'].round(1)
    def format_action(row, action_col, time_col):
        state = row[action_col]
        time = row.get(time_col, 0)
        if state == 1 and time > 0:
            return f"1({time})"
        return str(int(state))
    result_df['좋아요'] = result_df.apply(lambda row: format_action(row, '좋아요', 'time_to_like'), axis=1)
    result_df['싫어요'] = result_df.apply(lambda row: format_action(row, '싫어요', 'time_to_dislike'), axis=1)
    if search_login_id: result_df = result_df[result_df['login_id'].str.contains(search_login_id, na=False)]
    if search_shorts_url: result_df = result_df[result_df['shorts_url'].str.contains(search_shorts_url, na=False)]
    result_df = result_df[final_columns]
    return result_df.to_dict('records'), final_columns

def super_admin_required(f):
    def decorated_function(*args, **kwargs):
        if session.get('user_role') != 'super_admin': return redirect(url_for('admin_page'))
        return f(*args, **kwargs)
    decorated_function.__name__ = f.__name__
    return decorated_function

def admin_access_required(f):
    def decorated_function(*args, **kwargs):
        if session.get('user_role') not in ['admin', 'super_admin']: return redirect(url_for('login_page'))
        return f(*args, **kwargs)
    decorated_function.__name__ = f.__name__
    return decorated_function

@app.route('/admin', methods=['GET', 'POST'])
@admin_access_required
def admin_page():
    inspector = inspect(db.engine)
    table_names = inspector.get_table_names()
    args = request.values
    table_name = args.get('table')
    search_login_id = args.get('search_login_id', '')
    search_shorts_url = args.get('search_shorts_url', '')
    page = args.get('page', 1, type=int)
    clear_success = args.get('clear_success')
    if not table_name: return render_template('admin.html', tables=table_names, clear_success=clear_success, user_role=session.get('user_role'))
    if table_name == 'measurement_results':
        data, columns = generate_measurement_results(search_login_id, search_shorts_url)
        return render_template('admin.html', tables=table_names, selected_table=table_name, columns=columns, data=data, search_login_id=search_login_id, search_shorts_url=search_shorts_url, user_role=session.get('user_role'))
    Model = MODELS.get(table_name)
    if not Model: return render_template('admin.html', tables=table_names, error="Table not found", user_role=session.get('user_role'))
    query = Model.query
    if table_name == 'youtube_comment':
        if search_login_id: query = query.filter(Model.author_name.like(f"%{search_login_id}%"))
        if search_shorts_url: query = query.filter(Model.shorts_url.like(f"%{search_shorts_url}%"))
    else:
        if search_login_id and hasattr(Model, 'login_id'): query = query.filter(Model.login_id.like(f"%{search_login_id}%"))
        if search_shorts_url and hasattr(Model, 'shorts_url'): query = query.filter(Model.shorts_url.like(f"%{search_shorts_url}%"))
    if table_name == 'event_log' and hasattr(Model, 'id'): query = query.order_by(Model.id.desc())
    pagination = query.paginate(page=page, per_page=100, error_out=False)
    results = pagination.items
    data = [{c.key: getattr(d, c.key) for c in inspect(Model).c} for d in results]
    columns = [c.key for c in inspect(Model).c]
    return render_template('admin.html', tables=table_names, selected_table=table_name, columns=columns, data=data, pagination=pagination, search_login_id=search_login_id, search_shorts_url=search_shorts_url, clear_success=clear_success, user_role=session.get('user_role'))

@app.route('/admin/download_excel', methods=['POST'])
@admin_access_required
def download_excel():
    table_name = request.form.get('table_name_for_download')
    search_login_id = request.form.get('search_login_id')
    search_shorts_url = request.form.get('search_shorts_url')
    if table_name == 'measurement_results':
        data, _ = generate_measurement_results(search_login_id, search_shorts_url)
        df = pd.DataFrame(data)
    else:
        Model = MODELS.get(table_name)
        if not Model: return "Table not found", 404
        query = Model.query
        if search_login_id and hasattr(Model, 'login_id'): query = query.filter(Model.login_id.like(f"%{search_login_id}%"))
        if search_shorts_url and hasattr(Model, 'shorts_url'): query = query.filter(Model.shorts_url.like(f"%{search_shorts_url}%"))
        results = query.all()
        data = [{c.key: getattr(d, c.key) for c in inspect(Model).c} for d in results]
        df = pd.DataFrame(data)
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name=table_name)
    writer.close()
    output.seek(0)
    return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=f'{table_name}.xlsx')

@app.route('/admin/clear_table', methods=['POST'])
@super_admin_required
def clear_table():
    table_name = request.form.get('table')
    if not table_name: return redirect(url_for('admin_page'))
    Model = MODELS.get(table_name)
    if not Model: return "Table not found", 404
    results = Model.query.all()
    if results:
        data = [{c.key: getattr(d, c.key) for c in inspect(Model).c} for d in results]
        df = pd.DataFrame(data)
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name=table_name)
        writer.close()
        output.seek(0)
        db.session.query(Model).delete()
        db.session.commit()
        timestamp = datetime.now(KST).strftime('%Y%m%d%H%M%S')
        filename = f"{table_name}_backup_{timestamp}.xlsx"
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=filename)
    else:
        message = f"'{table_name}' 테이블에 데이터가 없어 초기화를 진행하지 않았습니다."
        return redirect(url_for('admin_page', clear_success=message))

@app.route('/admin/upload_excel', methods=['POST'])
@super_admin_required
def upload_excel():
    table_name = request.form.get('table')
    file = request.files.get('excelFile')
    inspector = inspect(db.engine)
    table_names = inspector.get_table_names()
    if table_name not in ['login_user', 'shorts']: return render_template('admin.html', tables=table_names, selected_table=table_name, upload_error="이 테이블은 엑셀 업로드를 지원하지 않습니다.", user_role=session.get('user_role'))
    if not file or file.filename == '': return render_template('admin.html', tables=table_names, selected_table=table_name, upload_error="업로드할 파일을 선택하세요.", user_role=session.get('user_role'))
    Model = MODELS.get(table_name)
    try:
        df = pd.read_excel(file)
        db.session.query(Model).delete()
        for _, row in df.iterrows():
            new_record = Model(**row.to_dict())
            db.session.add(new_record)
        db.session.commit()
        return render_template('admin.html', tables=table_names, selected_table=table_name, upload_success=f"{len(df)}개의 행이 {table_name} 테이블에 업로드되었습니다.", user_role=session.get('user_role'))
    except Exception as e:
        db.session.rollback()
        return render_template('admin.html', tables=table_names, selected_table=table_name, upload_error=f"업로드 실패: {str(e)}", user_role=session.get('user_role'))

def crawl_comments_task():
    with app.app_context():
        global CRAWL_STATUS
        try:
            if not YOUTUBE_API_KEY:
                CRAWL_STATUS['progress'] = "오류 발생: YouTube API 키가 환경 변수에 설정되지 않았습니다."
                CRAWL_STATUS['is_running'] = False
                return
            youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
            shorts_to_crawl = Shorts.query.filter_by(use_yn='Y').all()
            total_videos = len(shorts_to_crawl)
            for i, short in enumerate(shorts_to_crawl):
                video_id = short.url.split('?')[0].split('/shorts/')[-1]
                CRAWL_STATUS['progress'] = f"({i+1}/{total_videos}) 영상 '{video_id}' 댓글 수집 중..."
                YoutubeComment.query.filter_by(shorts_url=short.url).delete()
                db.session.commit()
                request_obj = youtube.commentThreads().list(part="snippet,replies", videoId=video_id, maxResults=100)
                while request_obj:
                    response = request_obj.execute()
                    for item in response['items']:
                        comment = item['snippet']['topLevelComment']['snippet']
                        new_comment = YoutubeComment(shorts_url=short.url, comment_id=item['id'], parent_id=None, author_name=comment['authorDisplayName'], comment_text=comment['textDisplay'], published_at=comment['publishedAt'], like_count=comment['likeCount'], author_profile_image_url=comment['authorProfileImageUrl'])
                        db.session.add(new_comment)
                        if 'replies' in item:
                            for reply_item in item['replies']['comments']:
                                reply = reply_item['snippet']
                                new_reply = YoutubeComment(shorts_url=short.url, comment_id=reply_item['id'], parent_id=item['id'], author_name=reply['authorDisplayName'], comment_text=reply['textDisplay'], published_at=reply['publishedAt'], like_count=reply['likeCount'], author_profile_image_url=reply['authorProfileImageUrl'])
                                db.session.add(new_reply)
                    db.session.commit()
                    request_obj = youtube.commentThreads().list_next(request_obj, response)
            CRAWL_STATUS['progress'] = f"완료: 총 {total_videos}개 영상의 댓글 수집 완료."
        except Exception as e:
            db.session.rollback()
            CRAWL_STATUS['progress'] = f"오류 발생: {e}"
        finally:
            CRAWL_STATUS['is_running'] = False

@app.route('/admin/start_crawl', methods=['POST'])
@super_admin_required
def start_crawl():
    global CRAWL_STATUS
    if CRAWL_STATUS['is_running']: return jsonify({'status': 'error', 'message': '이미 크롤링이 진행 중입니다.'})
    CRAWL_STATUS['is_running'] = True
    CRAWL_STATUS['progress'] = '크롤링 시작...'
    thread = threading.Thread(target=crawl_comments_task)
    thread.start()
    return jsonify({'status': 'success', 'message': '댓글 크롤링을 시작했습니다.'})

@app.route('/admin/crawl_status')
@admin_access_required
def crawl_status():
    return jsonify(CRAWL_STATUS)


# --- 데이터베이스 명령어 ---
@app.cli.command("init_db")
def init_db_command():
    """데이터베이스 테이블을 생성합니다."""
    with app.app_context():
        db.create_all()
        print("Initialized the database.")

@app.cli.command("reset_comments")
def reset_comments_command():
    """youtube_comment 테이블을 삭제하고 다시 생성합니다."""
    with app.app_context():
        table = YoutubeComment.__table__
        print("Dropping youtube_comment table...")
        table.drop(db.engine, checkfirst=True)
        print("Recreating youtube_comment table...")
        table.create(db.engine)
        print("youtube_comment table recreated successfully.")

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)