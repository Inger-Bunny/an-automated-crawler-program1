import requests
import parsel
import time
import logging
import sqlite3

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def extract_house_info(li):
    """从单个房源项中提取信息"""
    try:
        title = li.css('.title a::text').get()
        href = li.css('.title a::attr(href)').get()
        area_list = li.css('.flood a::text').getall()
        area = '_'.join(area_list)
        house_info = li.css('.houseInfo::text').get().split('|')
        unit_type = house_info[0] if len(house_info) > 0 else '无户型'
        acreage = house_info[1] if len(house_info) > 1 else '无面积'
        path = house_info[2] if len(house_info) > 2 else '无朝向'
        furnish = house_info[3] if len(house_info) > 3 else '无装修'
        floor = house_info[4] if len(house_info) > 4 else '无楼层'
        time_1 = house_info[5] if len(house_info) > 5 else '无建立时间'
        house_type = house_info[6] if len(house_info) > 6 else '无房子类型'
        follow_info = li.css('.followInfo::text').get().split('/')
        follow_man = follow_info[0] if len(follow_info) > 0 else '无关注人数'
        updated = follow_info[1] if len(follow_info) > 1 else '无发布时间'
        tag_list = li.css('.tag span::text').getall()
        tag = '_'.join(tag_list) if tag_list else '无标签'
        total_price = (li.css('.totalPrice span::text').get() or '无总价') + '万'
        unit_price = (li.css('.unitPrice span::text').get() or '无单价').replace('单价', '')

        return {
            '标题': title,
            '地区': area,
            '户型': unit_type,
            '面积': acreage,
            '朝向': path,
            '装修': furnish,
            '楼层': floor,
            '建立时间': time_1,
            '房子类型': house_type,
            '标签': tag,
            '总价': total_price,
            '单价': unit_price,
            '关注人数': follow_man,
            '发布时间': updated,
            '详情页': href,
        }
    except Exception as e:
        logging.error(f"提取数据出错: {e}")
        return None
def scrape_page(page):
    """爬取单个页面的数据"""
    url = f'https://sh.lianjia.com/ershoufang/pg{page}/'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'
    }

    try:
        response = requests.get(url=url, headers=headers)
        response.raise_for_status()
        selector = parsel.Selector(response.text)
        lis = selector.css('.sellListContent li')
        for li in lis:
            house_info = extract_house_info(li)
            if house_info:
                yield house_info
    except requests.RequestException as e:
        logging.error(f"请求失败: {e}")
def create_table_if_not_exists():
    """创建数据库表"""
    conn = sqlite3.connect('house_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS houses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            area TEXT,
            unit_type TEXT,
            acreage TEXT,
            path TEXT,
            furnish TEXT,
            floor TEXT,
            time TEXT,
            house_type TEXT,
            tag TEXT,
            total_price TEXT,
            unit_price TEXT,
            follow_man TEXT,
            updated TEXT,
            href TEXT UNIQUE
        )
    ''')
    conn.commit()
    conn.close()
def insert_data(house_info):
    """插入数据到数据库"""
    conn = sqlite3.connect('house_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO houses (title, area, unit_type, acreage, path, furnish, floor, time, 
                                      house_type, tag, total_price, unit_price, follow_man, updated, href)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        house_info['标题'], house_info['地区'], house_info['户型'], house_info['面积'], house_info['朝向'],
        house_info['装修'], house_info['楼层'], house_info['建立时间'], house_info['房子类型'],
        house_info['标签'], house_info['总价'], house_info['单价'], house_info['关注人数'],
        house_info['发布时间'], house_info['详情页']
    ))
    conn.commit()
    conn.close()
def load_existing_data():
    """从数据库加载已存在的数据"""
    existing_data = set()
    conn = sqlite3.connect('house_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT href FROM houses')
    rows = cursor.fetchall()
    for row in rows:
        existing_data.add(row[0])
    conn.close()
    return existing_data
def main():
    create_table_if_not_exists()
    existing_data = load_existing_data()

    for page in range(1, 37):
        logging.info(f"正在爬取第 {page} 页数据...")
        page_data_written = False
        for house_info in scrape_page(page):
            if house_info and house_info['详情页'] not in existing_data:
                insert_data(house_info)
                logging.info(f"成功写入数据: {house_info['标题']}")
                existing_data.add(house_info['详情页'])
                page_data_written = True

        if not page_data_written:
            logging.warning(f"第 {page} 页没有数据被写入")

        time.sleep(1)
if __name__ == '__main__':
    main()
