# -*- coding: utf-8 -*-
"""
天气数据爬虫模块
功能：爬取历史天气和AQI数据、数据分析可视化、导出CSV
优化版：支持进度回调、会话复用、增强错误处理、分析函数健壮性
"""
import os
import time
import warnings
import logging
import requests
import pymysql
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from lxml import etree
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime

warnings.filterwarnings('ignore')

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('weather_crawler.log', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ---------- 全局配置 ----------
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 3306)),
    'user': os.environ.get('DB_USER', 'root'),
    'password': os.environ.get('DB_PASSWORD', '123456'),
    'database': os.environ.get('DB_NAME', 'weather_db'),
    'charset': 'utf8mb4'
}

# 城市列表（拼音: 中文名）
CITIES = {
    "beijing": "北京",
    "shanghai": "上海",
    "guangzhou": "广州",
    "shenzhen": "深圳"
}

# 时间范围
YEARS = range(2020, 2025)
MONTHS = [f"{i:02d}" for i in range(1, 13)]

# 请求配置
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'
}
MAX_WORKERS = 4
MAX_RETRIES = 3
TIMEOUT = 15
REQUEST_DELAY = 0.5


def setup_chinese_font():
    """配置matplotlib中文显示"""
    import matplotlib as mpl
    font_list = ['SimHei', 'Microsoft YaHei', 'PingFang SC', 'Heiti TC', 'DejaVu Sans', 'Arial']
    available = set(f.name for f in mpl.font_manager.fontManager.ttflist)
    selected = next((f for f in font_list if f in available), 'DejaVu Sans')
    mpl.rcParams['font.sans-serif'] = [selected]
    mpl.rcParams['axes.unicode_minus'] = False
    mpl.rcParams['font.size'] = 10
    logger.info(f"字体配置完成，使用字体: {selected}")


setup_chinese_font()


@contextmanager
def get_db_cursor():
    """数据库游标上下文"""
    conn = None
    cursor = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        yield cursor, conn
    except pymysql.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"数据库操作出错: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def create_tables_if_not_exists():
    """创建数据表（如果不存在）"""
    create_weather = """
    CREATE TABLE IF NOT EXISTS history_weather (
        id INT AUTO_INCREMENT PRIMARY KEY,
        city_name VARCHAR(50) NOT NULL,
        date DATE NOT NULL,
        day_weather_condition VARCHAR(50),
        night_weather_condition VARCHAR(50),
        max_temperature DECIMAL(4,1),
        min_temperature DECIMAL(4,1),
        day_wind_direction VARCHAR(20),
        night_wind_direction VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY idx_city_date (city_name, date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    create_aqi = """
    CREATE TABLE IF NOT EXISTS history_aqi (
        id INT AUTO_INCREMENT PRIMARY KEY,
        city_name VARCHAR(50) NOT NULL,
        date DATE NOT NULL,
        aqi_quality_grade VARCHAR(20),
        aqi_index INT,
        aqi_ranking_day INT,
        PM25 DECIMAL(6,1),
        PM10 DECIMAL(6,1),
        So2 DECIMAL(6,1),
        No2 DECIMAL(6,1),
        Co DECIMAL(6,1),
        O3 DECIMAL(6,1),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY idx_city_date (city_name, date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        with get_db_cursor() as (cursor, conn):
            cursor.execute(create_weather)
            cursor.execute(create_aqi)
            conn.commit()
        logger.info("数据表创建/检查完成")
        return True
    except Exception as e:
        logger.error(f"创建数据表失败: {e}")
        return False


def check_database_status():
    """检查数据库状态"""
    try:
        with get_db_cursor() as (cursor, conn):
            cursor.execute("SELECT COUNT(*) FROM history_weather")
            weather_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM history_aqi")
            aqi_count = cursor.fetchone()[0]
            print(f"\n数据库状态: weather表 {weather_count} 条, aqi表 {aqi_count} 条")
            if weather_count > 0:
                cursor.execute("SELECT city_name, COUNT(*) FROM history_weather GROUP BY city_name")
                for city, cnt in cursor.fetchall():
                    print(f"  {city}: {cnt} 条")
            return True
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return False


def clear_tables():
    """清空数据表"""
    try:
        with get_db_cursor() as (cursor, conn):
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            cursor.execute("TRUNCATE TABLE history_weather")
            cursor.execute("TRUNCATE TABLE history_aqi")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            conn.commit()
        logger.info("已清空数据表")
    except Exception as e:
        logger.error(f"清空表时出错: {e}")


def safe_request(url, session=None):
    """带重试的请求（支持会话复用）"""
    retries = 0
    delay = 5
    last_exception = None
    while retries < MAX_RETRIES:
        try:
            if session:
                resp = session.get(url, timeout=TIMEOUT)
            else:
                resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            time.sleep(REQUEST_DELAY)
            return resp
        except requests.exceptions.RequestException as e:
            retries += 1
            last_exception = e
            if retries < MAX_RETRIES:
                logger.warning(f"请求失败，第{retries}次重试: {url}")
                time.sleep(delay)
                delay *= 2
            else:
                logger.error(f"请求失败，已达最大重试次数: {url}")
    raise last_exception


def crawl_and_save_history_weather(city_pinyin, city_name, year, month):
    """爬取历史天气数据并保存"""
    url = f'http://www.tianqihoubao.com/lishi/{city_pinyin}/month/{year}{month}.html'
    try:
        with requests.Session() as session:
            session.headers.update(HEADERS)
            resp = safe_request(url, session)
            html = etree.HTML(resp.text)
            trs = html.xpath('//table[contains(@class, "weather")]//tr')
            if not trs:
                trs = html.xpath('//tr[td[1][contains(text(), "年")]]')
            if not trs:
                return 0, "未找到数据行"

            weather_data = []
            for tr in trs:
                tds = tr.xpath('.//td')
                if len(tds) < 4:
                    continue
                date_text = ''.join(tds[0].xpath('.//text()')).strip()
                weather_text = ''.join(tds[1].xpath('.//text()')).strip()
                temp_text = ''.join(tds[2].xpath('.//text()')).strip()
                wind_text = ''.join(tds[3].xpath('.//text()')).strip()
                if '日期' in date_text or not date_text:
                    continue
                if '年' in date_text:
                    date = date_text.replace('年', '-').replace('月', '-').replace('日', '').strip()
                else:
                    continue
                if ' / ' in weather_text:
                    day_weather, night_weather = weather_text.split(' / ', 1)
                else:
                    day_weather = night_weather = weather_text
                max_temp = min_temp = None
                if temp_text:
                    temp_clean = temp_text.replace('℃', '').strip()
                    if '/' in temp_clean:
                        parts = temp_clean.split('/')
                        try:
                            max_temp = float(parts[0].strip()) if parts[0].strip() else None
                            min_temp = float(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else None
                        except ValueError:
                            pass
                if ' / ' in wind_text:
                    day_wind, night_wind = wind_text.split(' / ', 1)
                else:
                    day_wind = night_wind = wind_text
                weather_data.append({
                    'city_name': city_name, 'date': date, 'day_weather': day_weather,
                    'night_weather': night_weather, 'max_temp': max_temp, 'min_temp': min_temp,
                    'day_wind': day_wind, 'night_wind': night_wind
                })

            if weather_data:
                with get_db_cursor() as (cursor, conn):
                    insert_sql = """
                    INSERT INTO history_weather 
                    (city_name, date, day_weather_condition, night_weather_condition, 
                     max_temperature, min_temperature, day_wind_direction, night_wind_direction) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    day_weather_condition = VALUES(day_weather_condition),
                    night_weather_condition = VALUES(night_weather_condition),
                    max_temperature = VALUES(max_temperature),
                    min_temperature = VALUES(min_temperature),
                    day_wind_direction = VALUES(day_wind_direction),
                    night_wind_direction = VALUES(night_wind_direction)
                    """
                    for d in weather_data:
                        cursor.execute(insert_sql, (d['city_name'], d['date'], d['day_weather'], d['night_weather'],
                                                    d['max_temp'], d['min_temp'], d['day_wind'], d['night_wind']))
                    conn.commit()
                    logger.info(f"{city_name} {year}-{month}: 成功处理 {len(weather_data)} 条天气数据")
                return len(weather_data), "成功"
            return 0, "无有效数据"
    except Exception as e:
        logger.error(f"{city_name} {year}-{month} 天气爬取失败: {e}")
        return 0, f"错误: {str(e)[:100]}"


def crawl_and_save_history_aqi(city_pinyin, city_name, year, month):
    """爬取历史空气质量数据并保存"""
    url = f'http://www.tianqihoubao.com/aqi/{city_pinyin}-{year}{month}.html'
    try:
        with requests.Session() as session:
            session.headers.update(HEADERS)
            resp = safe_request(url, session)
            html = etree.HTML(resp.text)
            trs = html.xpath('//table//tr')[1:]
            if not trs:
                return 0, "未找到AQI数据行"

            aqi_data = []
            for tr in trs:
                tds = tr.xpath('./td')
                if len(tds) < 10:
                    continue
                date_text = ''.join(tds[0].xpath('./text()')).strip()
                if not date_text:
                    continue
                if '年' in date_text:
                    date = date_text.replace('年', '-').replace('月', '-').replace('日', '').strip()
                else:
                    date = date_text

                def parse_numeric(v):
                    try:
                        return float(v) if v and v.strip() else None
                    except:
                        return None

                aqi_data.append({
                    'date': date,
                    'aqi_index': parse_numeric(''.join(tds[1].xpath('./text()')).strip()),
                    'aqi_grade': ''.join(tds[2].xpath('./text()')).strip(),
                    'aqi_ranking': parse_numeric(''.join(tds[3].xpath('./text()')).strip()),
                    'pm25': parse_numeric(''.join(tds[4].xpath('./text()')).strip()),
                    'pm10': parse_numeric(''.join(tds[5].xpath('./text()')).strip()),
                    'so2': parse_numeric(''.join(tds[6].xpath('./text()')).strip()),
                    'no2': parse_numeric(''.join(tds[7].xpath('./text()')).strip()),
                    'co': parse_numeric(''.join(tds[8].xpath('./text()')).strip()),
                    'o3': parse_numeric(''.join(tds[9].xpath('./text()')).strip())
                })

            if aqi_data:
                with get_db_cursor() as (cursor, conn):
                    insert_sql = """
                    INSERT INTO history_aqi 
                    (city_name, date, aqi_quality_grade, aqi_index, aqi_ranking_day, 
                     PM25, PM10, So2, No2, Co, O3) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    aqi_quality_grade = VALUES(aqi_quality_grade),
                    aqi_index = VALUES(aqi_index),
                    aqi_ranking_day = VALUES(aqi_ranking_day),
                    PM25 = VALUES(PM25),
                    PM10 = VALUES(PM10),
                    So2 = VALUES(So2),
                    No2 = VALUES(No2),
                    Co = VALUES(Co),
                    O3 = VALUES(O3)
                    """
                    for d in aqi_data:
                        if d['date'] and d['aqi_index'] is not None:
                            cursor.execute(insert_sql, (city_name, d['date'], d['aqi_grade'], d['aqi_index'],
                                                        d['aqi_ranking'], d['pm25'], d['pm10'], d['so2'],
                                                        d['no2'], d['co'], d['o3']))
                    conn.commit()
                    logger.info(f"{city_name} {year}-{month}: 成功处理 {len(aqi_data)} 条AQI数据")
                return len(aqi_data), "成功"
            return 0, "无有效AQI数据"
    except Exception as e:
        logger.error(f"{city_name} {year}-{month} AQI爬取失败: {e}")
        return 0, f"错误: {str(e)[:100]}"


def execute_crawling(progress_callback=None):
    """
    执行所有爬取任务（多线程）
    progress_callback: 回调函数，参数为(completed, total, current_task_name)
    返回: (是否成功, 总任务数, 成功任务数)
    """
    tasks = []
    for pinyin, name in CITIES.items():
        for year in YEARS:
            for month in MONTHS:
                tasks.append(('weather', pinyin, name, str(year), month))
                tasks.append(('aqi', pinyin, name, str(year), month))

    total = len(tasks)
    logger.info(f"共生成 {total} 个爬取任务")
    logger.info(f"城市: {', '.join(CITIES.values())}")
    logger.info(f"时间范围: {YEARS[0]}年 至 {YEARS[-1]}年")
    print("开始爬取，请耐心等待...")

    start_time = time.time()
    success_count = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for task in tasks:
            task_type, pinyin, name, year, month = task
            if task_type == 'weather':
                future = executor.submit(crawl_and_save_history_weather, pinyin, name, year, month)
            else:
                future = executor.submit(crawl_and_save_history_aqi, pinyin, name, year, month)
            futures[future] = (task_type, name, year, month)

        for future in as_completed(futures):
            completed += 1
            task_type, name, year, month = futures[future]
            current_task = f"{name} {year}-{month} {task_type}"
            try:
                cnt, msg = future.result()
                if cnt > 0:
                    success_count += 1
                    status = "成功"
                else:
                    status = f"失败: {msg}"
                    logger.warning(f"任务失败: {current_task} - {msg}")
            except Exception as e:
                logger.error(f"任务执行异常: {current_task} - {e}")
                status = f"异常: {str(e)[:50]}"

            if progress_callback:
                progress_callback(completed, total, current_task)

            if completed % 10 == 0:
                logger.info(f"进度: {completed / total * 100:.1f}% ({completed}/{total})")

    elapsed = time.time() - start_time
    logger.info(f"爬取完成，耗时：{elapsed:.2f} 秒，成功任务数: {success_count}/{total}")
    return success_count > 0, total, success_count


def analyze_weather_aqi_separate():
    """生成多种分析图表（增强健壮性，避免空数据崩溃）"""
    logger.info("开始执行数据分析...")
    try:
        with get_db_cursor() as (cursor, conn):
            df_w = pd.read_sql('SELECT * FROM history_weather', conn, parse_dates=['date'])
            df_a = pd.read_sql('SELECT * FROM history_aqi', conn, parse_dates=['date'])

        if df_w.empty and df_a.empty:
            logger.warning("数据为空，无法分析")
            print("⚠️ 数据库中无数据，请先爬取数据。")
            return

        for col in ['max_temperature', 'min_temperature']:
            if col in df_w.columns:
                df_w[col] = pd.to_numeric(df_w[col], errors='coerce')
        for col in ['aqi_index', 'PM25', 'PM10', 'So2', 'No2', 'Co', 'O3']:
            if col in df_a.columns:
                df_a[col] = pd.to_numeric(df_a[col], errors='coerce')

        if not df_w.empty:
            print("\n温度统计:\n", df_w[['max_temperature', 'min_temperature']].describe())
        if not df_a.empty:
            print("\nAQI统计:\n", df_a[['aqi_index', 'PM25', 'PM10']].describe())

        out_dir = 'weather_analysis_results'
        os.makedirs(out_dir, exist_ok=True)
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']

        # 图表1: 月度平均AQI趋势
        if not df_a.empty:
            plt.figure(figsize=(12, 6))
            df_a['year_month'] = df_a['date'].dt.to_period('M')
            monthly = df_a.groupby(['city_name', 'year_month'])['aqi_index'].mean().unstack(0)
            monthly.index = monthly.index.astype(str)
            for i, city in enumerate(monthly.columns):
                plt.plot(monthly.index, monthly[city], marker='o', markersize=4, linewidth=2,
                         label=city, color=colors[i % len(colors)])
            plt.title('各城市月度平均AQI趋势（2020-2024）', fontsize=14)
            plt.xlabel('年月')
            plt.ylabel('平均AQI指数')
            plt.xticks(rotation=45)
            plt.legend()
            plt.grid(alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(out_dir, 'monthly_aqi_trend.png'), dpi=300, bbox_inches='tight')
            plt.close()
            print("✓ 图表1 - 月度AQI趋势图已保存")

        # 图表2: AQI分布箱线图
        if not df_a.empty:
            plt.figure(figsize=(10, 6))
            sns.boxplot(x='city_name', y='aqi_index', data=df_a, palette='Set2')
            plt.title('各城市AQI指数分布')
            plt.xlabel('城市')
            plt.ylabel('AQI指数')
            plt.grid(alpha=0.3, axis='y')
            plt.tight_layout()
            plt.savefig(os.path.join(out_dir, 'city_aqi_distribution.png'), dpi=300, bbox_inches='tight')
            plt.close()
            print("✓ 图表2 - AQI分布箱线图已保存")

        # 图表3: 温度变化趋势
        if not df_w.empty:
            df_w['year_month'] = df_w['date'].dt.to_period('M')
            monthly_temp = df_w.groupby(['city_name', 'year_month'])[['max_temperature', 'min_temperature']].mean().unstack(0)
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8))
            if 'max_temperature' in monthly_temp.columns:
                max_data = monthly_temp['max_temperature']
                max_data.index = max_data.index.astype(str)
                for i, city in enumerate(max_data.columns):
                    ax1.plot(max_data.index, max_data[city], marker='o', markersize=3, linewidth=1.5,
                             label=city, color=colors[i % len(colors)])
                ax1.set_title('各城市月度最高温度趋势')
                ax1.set_ylabel('最高温度（℃）')
                ax1.legend()
                ax1.grid(alpha=0.3)
            if 'min_temperature' in monthly_temp.columns:
                min_data = monthly_temp['min_temperature']
                min_data.index = min_data.index.astype(str)
                for i, city in enumerate(min_data.columns):
                    ax2.plot(min_data.index, min_data[city], marker='s', markersize=3, linewidth=1.5,
                             label=city, color=colors[i % len(colors)])
                ax2.set_title('各城市月度最低温度趋势')
                ax2.set_ylabel('最低温度（℃）')
                ax2.legend()
                ax2.grid(alpha=0.3)
            plt.xlabel('年月')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(out_dir, 'monthly_temperature_trend.png'), dpi=300, bbox_inches='tight')
            plt.close()
            print("✓ 图表3 - 温度趋势图已保存")

        # 图表4: 温度与PM2.5相关性
        if not df_w.empty and not df_a.empty:
            merged = pd.merge(df_w, df_a, on=['city_name', 'date'], how='inner')
            if not merged.empty:
                plt.figure(figsize=(10, 6))
                sns.scatterplot(x='max_temperature', y='PM25', hue='city_name', data=merged, alpha=0.7, s=60, palette='Set2')
                sns.regplot(x='max_temperature', y='PM25', data=merged, scatter=False, color='black', line_kws={'alpha': 0.5, 'linestyle': '--'})
                plt.title('最高温度与PM2.5浓度相关性')
                plt.xlabel('最高温度（℃）')
                plt.ylabel('PM2.5浓度')
                plt.grid(alpha=0.3)
                plt.tight_layout()
                plt.savefig(os.path.join(out_dir, 'temperature_pm25_correlation.png'), dpi=300, bbox_inches='tight')
                plt.close()
                corr = merged[['max_temperature', 'PM25']].corr().iloc[0, 1]
                print(f"✓ 图表4 - 温度与PM2.5相关性图已保存，相关系数: {corr:.4f}")

        # 图表5: 空气质量等级分布
        if not df_a.empty:
            def cat_aqi(aqi):
                if aqi <= 50:
                    return '优'
                if aqi <= 100:
                    return '良'
                if aqi <= 150:
                    return '轻度污染'
                if aqi <= 200:
                    return '中度污染'
                if aqi <= 300:
                    return '重度污染'
                return '严重污染'
            df_a['aqi_category'] = df_a['aqi_index'].apply(cat_aqi)
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))
            aqi_dist = df_a.groupby(['city_name', 'aqi_category']).size().unstack(fill_value=0)
            aqi_dist.plot(kind='bar', stacked=True, ax=ax1, colormap='RdYlGn_r')
            ax1.set_title('各城市空气质量等级分布')
            ax1.set_xlabel('城市')
            ax1.set_ylabel('天数')
            ax1.legend(title='等级', fontsize=9)
            ax1.grid(alpha=0.3, axis='y')
            overall = df_a['aqi_category'].value_counts().sort_index()
            colors_cat = ['#00FF00', '#90EE90', '#FFFF00', '#FFA500', '#FF0000', '#8B0000']
            bars = ax2.bar(range(len(overall)), overall.values, color=colors_cat[:len(overall)])
            ax2.set_title('总体空气质量等级分布')
            ax2.set_xlabel('等级')
            ax2.set_ylabel('天数')
            ax2.set_xticks(range(len(overall)))
            ax2.set_xticklabels(overall.index)
            for bar in bars:
                h = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width() / 2, h + 5, f'{int(h)}', ha='center', va='bottom')
            ax2.grid(alpha=0.3, axis='y')
            plt.tight_layout()
            plt.savefig(os.path.join(out_dir, 'air_quality_category.png'), dpi=300, bbox_inches='tight')
            plt.close()
            print("✓ 图表5 - 空气质量等级分布图已保存")

        # 图表6: 污染物浓度对比
        if not df_a.empty:
            pollutants = ['PM25', 'PM10', 'So2', 'No2', 'Co', 'O3']
            names = ['PM2.5', 'PM10', 'SO₂', 'NO₂', 'CO', 'O₃']
            pol_data = []
            for city in df_a['city_name'].unique():
                city_data = df_a[df_a['city_name'] == city]
                pol_data.append([city_data[p].mean() for p in pollutants])
            pol_df = pd.DataFrame(pol_data, index=df_a['city_name'].unique(), columns=names)
            plt.figure(figsize=(12, 8))
            pol_df.plot(kind='bar', width=0.8, colormap='Set3')
            plt.title('各城市主要污染物平均浓度对比')
            plt.xlabel('城市')
            plt.ylabel('平均浓度')
            plt.legend(title='污染物')
            plt.grid(alpha=0.3, axis='y')
            plt.tight_layout()
            plt.savefig(os.path.join(out_dir, 'pollutant_comparison.png'), dpi=300, bbox_inches='tight')
            plt.close()
            print("✓ 图表6 - 污染物浓度对比图已保存")

        # 图表7: 相关性热图
        if not df_w.empty and not df_a.empty:
            merged = pd.merge(df_w, df_a, on=['city_name', 'date'], how='inner')
            if not merged.empty:
                corr_cols = ['max_temperature', 'min_temperature', 'aqi_index', 'PM25', 'PM10', 'So2', 'No2', 'Co', 'O3']
                corr_data = merged[corr_cols].corr()
                plt.figure(figsize=(10, 8))
                sns.heatmap(corr_data, annot=True, cmap='coolwarm', center=0, square=True, linewidths=1,
                            cbar_kws={"shrink": 0.8}, fmt='.2f', annot_kws={'size': 9})
                plt.title('气象与空气质量指标相关性热图')
                plt.tight_layout()
                plt.savefig(os.path.join(out_dir, 'correlation_heatmap.png'), dpi=300, bbox_inches='tight')
                plt.close()
                print("✓ 图表7 - 相关性热图已保存")

        print(f"\n✓ 数据分析完成！图表已保存到 '{out_dir}' 文件夹")
    except Exception as e:
        logger.error(f"数据分析出错: {e}")
        import traceback
        traceback.print_exc()
        print(f"❌ 分析失败: {e}")


def export_to_csv():
    """导出数据到CSV文件"""
    try:
        with get_db_cursor() as (cursor, conn):
            df_w = pd.read_sql('SELECT * FROM history_weather', conn)
            df_a = pd.read_sql('SELECT * FROM history_aqi', conn)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if not df_w.empty:
            df_w.to_csv(f'weather_data_{ts}.csv', index=False, encoding='utf-8-sig')
            print(f"✓ 天气数据已导出: weather_data_{ts}.csv ({len(df_w)} 条)")
        if not df_a.empty:
            df_a.to_csv(f'aqi_data_{ts}.csv', index=False, encoding='utf-8-sig')
            print(f"✓ AQI数据已导出: aqi_data_{ts}.csv ({len(df_a)} 条)")
    except Exception as e:
        logger.error(f"导出失败: {e}")