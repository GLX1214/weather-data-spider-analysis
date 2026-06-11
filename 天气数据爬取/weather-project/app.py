# -*- coding: utf-8 -*-
"""
天气数据爬取分析系统 - Flask主应用
集成：爬虫、数据管理、分析、实时天气预报、AQI预测、地图可视化
"""
import os
import sys
import threading
import logging
import pandas as pd                 # 用于AQI预测数据处理
from functools import wraps
from contextlib import contextmanager
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
import pymysql
from pymysql import cursors
from flask import Flask, render_template, jsonify, request, session, redirect, url_for

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from weather_crawler import (
    create_tables_if_not_exists,
    check_database_status,
    clear_tables,
    execute_crawling,
    analyze_weather_aqi_separate,
    export_to_csv
)

# ---------- 日志配置 ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------- Flask初始化 ----------
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24).hex())

# ---------- 用户配置（使用哈希密码）----------
USERS = {
    'admin': {
        'password': generate_password_hash('123456'),
        'role': 'admin',
        'name': '管理员'
    },
    'test': {
        'password': generate_password_hash('test123'),
        'role': 'user',
        'name': '测试用户'
    }
}

# ---------- 全局爬虫进度（线程安全）----------
crawl_progress = {
    'status': 'idle',      # idle, running, completed, error
    'progress': 0,
    'current_task': '',
    'message': '',
    'total_tasks': 0,
    'completed_tasks': 0
}
progress_lock = threading.Lock()

# ---------- 数据库配置（支持环境变量）----------
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 3306)),
    'user': os.environ.get('DB_USER', 'root'),
    'password': os.environ.get('DB_PASSWORD', '123456'),
    'database': os.environ.get('DB_NAME', 'weather_db'),
    'charset': 'utf8mb4',
    'cursorclass': cursors.DictCursor,
    'autocommit': False
}

# ---------- 模型保存目录 ----------
MODEL_DIR = 'models'
os.makedirs(MODEL_DIR, exist_ok=True)


@contextmanager
def get_db_connection():
    """数据库连接上下文管理器（返回字典游标）"""
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        yield conn
    finally:
        if conn:
            conn.close()


@contextmanager
def get_db_cursor():
    """获取数据库游标（自动处理事务）"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            yield cursor, conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


# ---------- 登录装饰器 ----------
def login_required(role=None):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not session.get('logged_in'):
                if request.path.startswith('/api/'):
                    return jsonify({"success": False, "error": "未登录"}), 401
                return redirect(url_for('login_page', next=request.url))
            if role and session.get('role') != role:
                return jsonify({"success": False, "error": "权限不足"}), 403
            return f(*args, **kwargs)
        return decorated
    return decorator


# ---------- 页面路由 ----------
@app.route('/login', methods=['GET', 'POST'])
def login_page():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        user = USERS.get(username)
        if user and check_password_hash(user['password'], password):
            session.clear()
            session['logged_in'] = True
            session['username'] = username
            session['role'] = user['role']
            session['name'] = user['name']
            next_page = request.args.get('next')
            return redirect(next_page) if next_page else redirect(url_for('index'))
        return render_template('login.html', error='用户名或密码错误')
    if session.get('logged_in'):
        return redirect(url_for('index'))
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login_page'))


@app.route('/')
@login_required()
def index():
    return render_template('index.html',
                           username=session.get('name', session.get('username')),
                           role=session.get('role'))


@app.route('/data')
@login_required()
def data_manage():
    return render_template('data.html',
                           username=session.get('name', session.get('username')),
                           role=session.get('role'))


@app.route('/analysis')
@login_required()
def analysis():
    chart_dir = "static/charts"
    charts = []
    if os.path.exists(chart_dir):
        charts = [f for f in os.listdir(chart_dir) if f.endswith('.png')]
    return render_template('analysis.html', charts=charts,
                           username=session.get('name', session.get('username')),
                           role=session.get('role'))


@app.route('/realtime')
@login_required()
def realtime():
    return render_template('realtime.html',
                           username=session.get('name', session.get('username')),
                           role=session.get('role'))


# ---------- API：数据查询 ----------
@app.route('/api/weather_data')
@login_required()
def get_weather_data():
    page = max(1, int(request.args.get('page', 1)))
    per_page = min(100, max(1, int(request.args.get('per_page', 20))))
    search = request.args.get('search', '').strip()
    city = request.args.get('city', '').strip()
    start_date = request.args.get('start_date', '').strip()
    end_date = request.args.get('end_date', '').strip()

    try:
        with get_db_cursor() as (cursor, conn):
            conditions = []
            params = []
            if search:
                conditions.append("(city_name LIKE %s OR day_weather_condition LIKE %s OR night_weather_condition LIKE %s)")
                params.extend([f'%{search}%'] * 3)
            if city:
                conditions.append("city_name = %s")
                params.append(city)
            if start_date:
                conditions.append("date >= %s")
                params.append(start_date)
            if end_date:
                conditions.append("date <= %s")
                params.append(end_date)

            where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
            cursor.execute(f"SELECT COUNT(*) as total FROM history_weather {where_clause}", params)
            total = cursor.fetchone()['total']

            data_sql = f"""
                SELECT * FROM history_weather 
                {where_clause}
                ORDER BY date DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(data_sql, params + [per_page, (page - 1) * per_page])
            data = cursor.fetchall()

            cursor.execute("SELECT DISTINCT city_name FROM history_weather ORDER BY city_name")
            cities = [row['city_name'] for row in cursor.fetchall()]

            return jsonify({
                "success": True,
                "data": data,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page if total > 0 else 1,
                "cities": cities
            })
    except Exception as e:
        logger.error(f"获取天气数据失败: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/aqi_data')
@login_required()
def get_aqi_data():
    page = max(1, int(request.args.get('page', 1)))
    per_page = min(100, max(1, int(request.args.get('per_page', 20))))
    search = request.args.get('search', '').strip()
    city = request.args.get('city', '').strip()
    quality = request.args.get('quality', '').strip()
    start_date = request.args.get('start_date', '').strip()
    end_date = request.args.get('end_date', '').strip()

    try:
        with get_db_cursor() as (cursor, conn):
            conditions = []
            params = []
            if search:
                conditions.append("(city_name LIKE %s OR aqi_quality_grade LIKE %s)")
                params.extend([f'%{search}%'] * 2)
            if city:
                conditions.append("city_name = %s")
                params.append(city)
            if quality:
                conditions.append("aqi_quality_grade = %s")
                params.append(quality)
            if start_date:
                conditions.append("date >= %s")
                params.append(start_date)
            if end_date:
                conditions.append("date <= %s")
                params.append(end_date)

            where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
            cursor.execute(f"SELECT COUNT(*) as total FROM history_aqi {where_clause}", params)
            total = cursor.fetchone()['total']

            data_sql = f"""
                SELECT * FROM history_aqi 
                {where_clause}
                ORDER BY date DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(data_sql, params + [per_page, (page - 1) * per_page])
            data = cursor.fetchall()

            cursor.execute("SELECT DISTINCT city_name FROM history_aqi ORDER BY city_name")
            cities = [row['city_name'] for row in cursor.fetchall()]
            cursor.execute("SELECT DISTINCT aqi_quality_grade FROM history_aqi ORDER BY aqi_quality_grade")
            qualities = [row['aqi_quality_grade'] for row in cursor.fetchall()]

            return jsonify({
                "success": True,
                "data": data,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page if total > 0 else 1,
                "cities": cities,
                "qualities": qualities
            })
    except Exception as e:
        logger.error(f"获取AQI数据失败: {e}")
        return jsonify({"success": False, "error": str(e)})


# ---------- API：数据操作 ----------
@app.route('/api/weather/<int:id>', methods=['PUT', 'DELETE'])
@login_required()
def modify_weather(id):
    try:
        with get_db_cursor() as (cursor, conn):
            if request.method == 'DELETE':
                cursor.execute("DELETE FROM history_weather WHERE id = %s", (id,))
                return jsonify({"success": True, "message": "删除成功"})
            elif request.method == 'PUT':
                data = request.json
                allowed_fields = ['city_name', 'date', 'day_weather_condition', 'night_weather_condition',
                                  'max_temperature', 'min_temperature', 'day_wind_direction', 'night_wind_direction']
                fields = []
                values = []
                for key in allowed_fields:
                    if key in data and data[key] is not None:
                        fields.append(f"{key} = %s")
                        values.append(data[key])
                if not fields:
                    return jsonify({"success": False, "error": "无有效字段"})
                values.append(id)
                sql = f"UPDATE history_weather SET {', '.join(fields)} WHERE id = %s"
                cursor.execute(sql, values)
                return jsonify({"success": True, "message": "更新成功"})
    except Exception as e:
        logger.error(f"修改天气数据失败: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/aqi/<int:id>', methods=['PUT', 'DELETE'])
@login_required()
def modify_aqi(id):
    try:
        with get_db_cursor() as (cursor, conn):
            if request.method == 'DELETE':
                cursor.execute("DELETE FROM history_aqi WHERE id = %s", (id,))
                return jsonify({"success": True, "message": "删除成功"})
            elif request.method == 'PUT':
                data = request.json
                allowed_fields = ['city_name', 'date', 'aqi_quality_grade', 'aqi_index', 'aqi_ranking_day',
                                  'PM25', 'PM10', 'So2', 'No2', 'Co', 'O3']
                fields = []
                values = []
                for key in allowed_fields:
                    if key in data and data[key] is not None:
                        fields.append(f"{key} = %s")
                        values.append(data[key])
                if not fields:
                    return jsonify({"success": False, "error": "无有效字段"})
                values.append(id)
                sql = f"UPDATE history_aqi SET {', '.join(fields)} WHERE id = %s"
                cursor.execute(sql, values)
                return jsonify({"success": True, "message": "更新成功"})
    except Exception as e:
        logger.error(f"修改AQI数据失败: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/batch_delete', methods=['POST'])
@login_required(role='admin')
def batch_delete():
    data = request.json
    table = data.get('table')
    ids = data.get('ids', [])
    if table not in ['history_weather', 'history_aqi'] or not ids:
        return jsonify({"success": False, "error": "参数错误"})
    try:
        with get_db_cursor() as (cursor, conn):
            placeholders = ','.join(['%s'] * len(ids))
            cursor.execute(f"DELETE FROM {table} WHERE id IN ({placeholders})", tuple(ids))
            return jsonify({"success": True, "deleted": cursor.rowcount})
    except Exception as e:
        logger.error(f"批量删除失败: {e}")
        return jsonify({"success": False, "error": str(e)})


# ---------- API：统计与图表 ----------
@app.route('/api/statistics/overview')
@login_required()
def get_statistics_overview():
    try:
        with get_db_cursor() as (cursor, conn):
            cursor.execute("SELECT COUNT(*) as count FROM history_weather")
            weather_count = cursor.fetchone()['count']
            cursor.execute("SELECT COUNT(*) as count FROM history_aqi")
            aqi_count = cursor.fetchone()['count']
            cursor.execute("SELECT city_name, COUNT(*) as count FROM history_weather GROUP BY city_name")
            city_distribution = cursor.fetchall()

            # 使用数据库中实际的最大日期，而不是 CURDATE()
            cursor.execute("""
                SELECT DATE(date) as day, COUNT(*) as count 
                FROM history_weather 
                WHERE date >= (SELECT DATE(MAX(date)) - INTERVAL 6 DAY FROM history_weather)
                GROUP BY DATE(date) ORDER BY day
            """)
            recent_trend = cursor.fetchall()

            cursor.execute("""
                SELECT aqi_quality_grade, COUNT(*) as count 
                FROM history_aqi 
                GROUP BY aqi_quality_grade ORDER BY count DESC
            """)
            aqi_distribution = cursor.fetchall()

            cursor.execute("""
                SELECT AVG(max_temperature) as avg_max, AVG(min_temperature) as avg_min,
                       MAX(max_temperature) as max_temp, MIN(min_temperature) as min_temp
                FROM history_weather
            """)
            temp_stats = cursor.fetchone()

            # 确保温度数值为数字类型（避免前端 .toFixed 报错）
            if temp_stats.get('avg_max') is not None:
                temp_stats['avg_max'] = float(temp_stats['avg_max'])
            else:
                temp_stats['avg_max'] = 0
            if temp_stats.get('avg_min') is not None:
                temp_stats['avg_min'] = float(temp_stats['avg_min'])
            else:
                temp_stats['avg_min'] = 0
            if temp_stats.get('max_temp') is not None:
                temp_stats['max_temp'] = float(temp_stats['max_temp'])
            if temp_stats.get('min_temp') is not None:
                temp_stats['min_temp'] = float(temp_stats['min_temp'])

            return jsonify({
                "success": True,
                "weather_count": weather_count,
                "aqi_count": aqi_count,
                "city_distribution": city_distribution,
                "recent_trend": recent_trend,
                "aqi_distribution": aqi_distribution,
                "temp_stats": temp_stats
            })
    except Exception as e:
        logger.error(f"获取统计概览失败: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/statistics/city_comparison')
@login_required()
def get_city_comparison():
    cities = [c.strip() for c in request.args.get('cities', '').split(',') if c.strip()]
    metric = request.args.get('metric', 'aqi')
    if not cities:
        return jsonify({"success": False, "error": "请选择城市"})
    try:
        with get_db_cursor() as (cursor, conn):
            placeholders = ','.join(['%s'] * len(cities))
            if metric == 'aqi':
                cursor.execute(f"""
                    SELECT city_name, AVG(aqi_index) as avg_aqi, MIN(aqi_index) as min_aqi,
                           MAX(aqi_index) as max_aqi, COUNT(*) as count
                    FROM history_aqi WHERE city_name IN ({placeholders}) GROUP BY city_name
                """, tuple(cities))
            elif metric == 'temp':
                cursor.execute(f"""
                    SELECT city_name, AVG(max_temperature) as avg_max, AVG(min_temperature) as avg_min,
                           MAX(max_temperature) as max_temp, MIN(min_temperature) as min_temp
                    FROM history_weather WHERE city_name IN ({placeholders}) GROUP BY city_name
                """, tuple(cities))
            else:
                cursor.execute(f"""
                    SELECT city_name, AVG(PM25) as avg_pm25, AVG(PM10) as avg_pm10,
                           AVG(So2) as avg_so2, AVG(No2) as avg_no2
                    FROM history_aqi WHERE city_name IN ({placeholders}) GROUP BY city_name
                """, tuple(cities))
            data = cursor.fetchall()
            return jsonify({"success": True, "data": data})
    except Exception as e:
        logger.error(f"获取城市对比失败: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/charts/trend')
@login_required()
def get_trend_chart_data():
    city = request.args.get('city', '北京')
    metric = request.args.get('metric', 'aqi')
    period = request.args.get('period', 'month')
    try:
        with get_db_cursor() as (cursor, conn):
            if period == 'month':
                group_by = "DATE_FORMAT(date, '%%Y-%%m')"   # 双%转义
            elif period == 'week':
                group_by = "DATE_FORMAT(date, '%%Y-%%u')"
            else:
                group_by = "DATE(date)"
            if metric == 'aqi':
                cursor.execute(f"""
                    SELECT {group_by} as period, AVG(aqi_index) as value
                    FROM history_aqi WHERE city_name = %s
                    GROUP BY period ORDER BY period LIMIT 50
                """, (city,))
            else:
                cursor.execute(f"""
                    SELECT {group_by} as period, AVG(max_temperature) as max_temp, AVG(min_temperature) as min_temp
                    FROM history_weather WHERE city_name = %s
                    GROUP BY period ORDER BY period LIMIT 50
                """, (city,))
            data = cursor.fetchall()
            return jsonify({"success": True, "data": data, "period": period})
    except Exception as e:
        logger.error(f"获取趋势数据失败: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/charts/correlation')
@login_required()
def get_correlation_data():
    city = request.args.get('city', '北京')
    try:
        with get_db_cursor() as (cursor, conn):
            cursor.execute("""
                SELECT w.date, w.max_temperature, w.min_temperature,
                       a.aqi_index, a.PM25, a.PM10, a.So2, a.No2, a.Co, a.O3
                FROM history_weather w
                JOIN history_aqi a ON w.city_name = a.city_name AND w.date = a.date
                WHERE w.city_name = %s LIMIT 1000
            """, (city,))
            data = cursor.fetchall()
            return jsonify({"success": True, "data": data})
    except Exception as e:
        logger.error(f"获取相关性数据失败: {e}")
        return jsonify({"success": False, "error": str(e)})


# ---------- API：爬虫与控制 ----------
def update_progress(status=None, progress=None, current_task=None, message=None):
    """线程安全更新爬虫进度"""
    with progress_lock:
        if status is not None:
            crawl_progress['status'] = status
        if progress is not None:
            crawl_progress['progress'] = progress
        if current_task is not None:
            crawl_progress['current_task'] = current_task
        if message is not None:
            crawl_progress['message'] = message


def run_crawl_with_progress():
    """带进度回调的爬虫执行"""
    update_progress(status='running', progress=0, current_task='初始化...', message='爬虫任务启动')

    def progress_callback(completed, total, current_task_name):
        with progress_lock:
            crawl_progress['completed_tasks'] = completed
            crawl_progress['total_tasks'] = total
            crawl_progress['progress'] = int(completed / total * 100) if total > 0 else 0
            crawl_progress['current_task'] = current_task_name
            crawl_progress['message'] = f'正在执行: {current_task_name}'

    try:
        success, total, completed = execute_crawling(progress_callback)
        if success:
            update_progress(status='completed', progress=100, current_task='完成', message='所有爬取任务已完成')
        else:
            update_progress(status='error', message='部分任务失败，请查看日志')
    except Exception as e:
        logger.error(f"爬虫执行异常: {e}")
        update_progress(status='error', message=str(e))


@app.route('/api/init_db')
@login_required()
def init_db():
    result = create_tables_if_not_exists()
    return jsonify({"success": result})


@app.route('/api/clear_db')
@login_required(role='admin')
def clear_db():
    clear_tables()
    return jsonify({"success": True})


@app.route('/api/crawl')
@login_required()
def crawl_data():
    with progress_lock:
        if crawl_progress['status'] == 'running':
            return jsonify({"success": False, "message": "已有爬虫任务在运行"})
        # 重置进度
        crawl_progress['status'] = 'idle'
        crawl_progress['progress'] = 0
        crawl_progress['current_task'] = ''
        crawl_progress['message'] = ''
        crawl_progress['total_tasks'] = 0
        crawl_progress['completed_tasks'] = 0

    thread = threading.Thread(target=run_crawl_with_progress, daemon=True)
    thread.start()
    return jsonify({"success": True, "message": "爬虫已启动"})


@app.route('/api/crawl_progress')
@login_required()
def get_crawl_progress():
    with progress_lock:
        return jsonify({
            'status': crawl_progress['status'],
            'progress': crawl_progress['progress'],
            'current_task': crawl_progress['current_task'],
            'message': crawl_progress['message'],
            'total_tasks': crawl_progress['total_tasks'],
            'completed_tasks': crawl_progress['completed_tasks']
        })


@app.route('/api/analyze')
@login_required()
def run_analysis():
    try:
        analyze_weather_aqi_separate()
        # 复制生成的图表到静态目录
        src_dir = "weather_analysis_results"
        dst_dir = "static/charts"
        if os.path.exists(src_dir):
            os.makedirs(dst_dir, exist_ok=True)
            import shutil
            for file in os.listdir(src_dir):
                if file.endswith('.png'):
                    shutil.copy(os.path.join(src_dir, file), os.path.join(dst_dir, file))
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"分析失败: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/export')
@login_required()
def export_data():
    try:
        export_to_csv()
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"导出失败: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/clear_all_charts', methods=['POST'])
@login_required()
def clear_all_charts():
    chart_dir = "static/charts"
    try:
        if os.path.exists(chart_dir):
            for f in os.listdir(chart_dir):
                file_path = os.path.join(chart_dir, f)
                if os.path.isfile(file_path):
                    os.remove(file_path)
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"清除图表失败: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/aqi/forecast')
@login_required()
def get_aqi_forecast():
    """获取未来7天 AQI 预测值"""
    city = request.args.get('city', '北京')
    model_path = os.path.join(MODEL_DIR, f'{city}_aqi.pkl')
    if not os.path.exists(model_path):
        success, msg = train_city_aqi_model(city)
        if not success:
            return jsonify({"success": False, "error": f"模型训练失败: {msg}"})
    try:
        from prophet import Prophet
        model = joblib.load(model_path)
        future = model.make_future_dataframe(periods=7)
        forecast = model.predict(future)
        last_7 = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(7)
        result = []
        for _, row in last_7.iterrows():
            result.append({
                'date': row['ds'].strftime('%Y-%m-%d'),
                'yhat': float(row['yhat']),
                'yhat_lower': float(row['yhat_lower']),
                'yhat_upper': float(row['yhat_upper'])
            })
        return jsonify({"success": True, "data": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})



if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)