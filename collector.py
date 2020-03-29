import sqlite3
import time
import requests
import bs4


def get_data():
    try:
        req = requests.get("https://стопкоронавирус.рф/")
        return req.content
    except:
        return None


def get_last_check():
    conn = connect_db()
    curs = conn.cursor()
    curs.execute('''SELECT * FROM russian_general ORDER BY timestamp DESC LIMIT 0,1;''')
    last_record = curs.fetchone()
    conn.close()
    return last_record


def parse_data(raw_html):
    BSparser = bs4.BeautifulSoup(raw_html, 'html.parser').find('div', {'class': 'd-map'})

    scrap_data = {'date': ' '.join(BSparser.find('div', {'class': 'd-map__title'}).text.split(' ')[-2::])}

    fields = (("sick_total", "sick_day"), ("healed_total", "healed_day"), ("die_total", "die_day"))
    for i, counter in enumerate(BSparser.find('div', {'class': 'd-map__counter'}).find_all('div')):
        values = list(map(int, counter.find('h3').text.split('+')))
        scrap_data[fields[i][0]] = values[0]
        scrap_data[fields[i][1]] = 0 if len(values) == 1 else values[1]

    scrap_data['Cities'] = {}
    for tr in BSparser.find('table').find_all('tr'):
        scrap_data['Cities'][tr.find('th').text.strip()] = tuple([int(td.text) for td in tr.find_all('td')])

    return scrap_data


def connect_db():
    conn = sqlite3.connect('base.sqlite')
    curs = conn.cursor()
    curs.executescript("""
        create table if not exists russian_general (timestamp REAL,date TEXT,sick_total INTEGER, sick_day INTEGER, \
        healed_total INTEGER, healed_day INTEGER,die_total INTEGER,die_day INTEGER);
        create table if not exists russian_region (timestamp REAL,date TEXT,region TEXT,sick INTEGER, healed INTEGER, \
        die INTEGER);
        """)
    conn.commit()
    curs.close()
    return conn


def update_db(data):
    conn = connect_db()
    curs = conn.cursor()
    record_data = (
        time.time(), data['date'], data['sick_total'], data['sick_day'], data['healed_total'], data['healed_day'],
        data['die_total'], data['die_day'])
    curs.execute("insert into russian_general values (?,?,?,?,?,?,?,?);", record_data)
    conn.commit()
    for city in data['Cities']:
        record_data = (
            time.time(), data['date'], city, data['Cities'][city][0], data['Cities'][city][1],
            data['Cities'][city][2])
        curs.execute("insert into russian_region values (?,?,?,?,?,?);", record_data)
    conn.commit()
    conn.close()


def update_stat():
    raw_content = get_data()
    if raw_content:
        data = parse_data(raw_content)
        last_check = get_last_check()
        if not last_check or (last_check[1] != data['date'] and last_check[2] != data['sick_total']):
            update_db(data)
            return True
    return False


if __name__ == '__main__':
    print(update_stat())
